import asyncio, json, os, random, re, sys, time, math
# Embedding provider selection:
# - If running in CI or SIMPLE_EMBEDDINGS env var is set, prefer `mock_embeddings`.
# - Otherwise try to import a real `embeddings.py` provider if present.
compute_embedding_fn = None
if os.environ.get('CI') or os.environ.get('SIMPLE_EMBEDDINGS'):
    try:
        from mock_embeddings import compute_embedding
        compute_embedding_fn = compute_embedding
    except Exception:
        compute_embedding_fn = None

if compute_embedding_fn is None:
    try:
        from embeddings import compute_embedding
        compute_embedding_fn = compute_embedding
    except Exception:
        compute_embedding_fn = None

class TinyDBNode:
    def __init__(self, host, port, db_file, peers=None):
        self.host, self.port, self.db_file = host, port, db_file
        # peers: list of (host, port)
        self.peers = peers or []
        # Leaderless mode: every node can accept writes.
        # We assign a per-node sequence and per-entry version for conflict resolution.
        self.seq = 0
        # metadata per key: version tuple {'ts': float, 'node': port, 'seq': int}
        self.meta = {}
        self.data = {}
        self.inverted_index = {}
        self.embeddings = {}
        # Track which words belong to which keys for proper cleanup
        self.key_to_words = {}
        # TF/IDF support
        self.doc_term_freqs = {}   # key -> {term: count}
        self.term_doc_freqs = {}   # term -> doc freq
        self.doc_lengths = {}      # key -> total terms
        self._load_from_wal()

    def _load_from_wal(self):
        if not os.path.exists(self.db_file):
            return
        # Read framed WAL: 4-byte big-endian length + payload
        try:
            with open(self.db_file, "rb") as f:
                while True:
                    header = f.read(4)
                    if not header or len(header) < 4:
                        break
                    length = int.from_bytes(header, "big")
                    payload = f.read(length)
                    if not payload or len(payload) < length:
                        # incomplete record at EOF - stop
                        break
                    try:
                        entry = json.loads(payload.decode())
                        self._apply_to_memory(entry)
                    except Exception:
                        continue
        except Exception:
            return

    def _compare_vc(self, a, b):
        """Compare two vector clocks a and b.
        Return 1 if a > b, -1 if a < b, 0 if equal, 2 if concurrent.
        """
        if not a and not b:
            return 0
        a = a or {}
        b = b or {}
        greater = False
        less = False
        keys = set(list(a.keys()) + list(b.keys()))
        for k in keys:
            av = a.get(k, 0)
            bv = b.get(k, 0)
            if av > bv:
                greater = True
            elif av < bv:
                less = True
        if greater and not less:
            return 1
        if less and not greater:
            return -1
        if not greater and not less:
            return 0
        return 2

    def _apply_to_memory(self, entry):
        cmd = entry.get('cmd')
        if cmd == "SET":
            key, val = entry['key'], entry['val']
            # Clean up old indexes for this key if it exists
            old_vc = None
            if key in self.meta and isinstance(self.meta[key].get('vc'), dict):
                old_vc = self.meta[key]['vc']
            incoming_vc = entry.get('_vc') if isinstance(entry.get('_vc'), dict) else None
            cmp = self._compare_vc(incoming_vc, old_vc)
            if cmp == -1:
                # incoming is older - ignore
                return
            if cmp == 2:
                # concurrent - deterministic tie-breaker: choose higher (node,seq) pair
                # pick the max (node, seq) from incoming_vc and old_vc
                def max_pair(vc):
                    if not vc: return (0, 0)
                    items = [(int(n), vc[n]) for n in vc]
                    return max(items)
                in_pair = max_pair(incoming_vc)
                old_pair = max_pair(old_vc)
                if in_pair < old_pair:
                    return
            # apply
            self._cleanup_indexes(key)
            self.data[key] = val
            if incoming_vc:
                self.meta[key] = {'vc': incoming_vc}
            self._index_data(key, val)
        elif cmd == "DEL":
            key = entry['key']
            old_vc = None
            if key in self.meta and isinstance(self.meta[key].get('vc'), dict):
                old_vc = self.meta[key]['vc']
            incoming_vc = entry.get('_vc') if isinstance(entry.get('_vc'), dict) else None
            cmp = self._compare_vc(incoming_vc, old_vc)
            if cmp == -1:
                return
            if key in self.data:
                del self.data[key]
            if key in self.meta:
                del self.meta[key]
            # remove from indexes properly
            self._cleanup_indexes(key)
        elif cmd == "BULK":
            incoming_vc = entry.get('_vc') if isinstance(entry.get('_vc'), dict) else None
            for k, v in entry['data']:
                old_vc = None
                if k in self.meta and isinstance(self.meta[k].get('vc'), dict):
                    old_vc = self.meta[k]['vc']
                cmp = self._compare_vc(incoming_vc, old_vc)
                if cmp == -1:
                    continue
                if cmp == 2:
                    def max_pair(vc):
                        if not vc: return (0, 0)
                        items = [(int(n), vc[n]) for n in vc]
                        return max(items)
                    in_pair = max_pair(incoming_vc)
                    old_pair = max_pair(old_vc)
                    if in_pair < old_pair:
                        continue
                self._cleanup_indexes(k)
                self.data[k] = v
                if incoming_vc:
                    self.meta[k] = {'vc': incoming_vc}
                self._index_data(k, v)

    def _cleanup_indexes(self, key):
        """Properly cleanup all indexes for a key"""
        # Remove from inverted index using tracked words
        if key in self.key_to_words:
            for w in self.key_to_words[key]:
                if w in self.inverted_index:
                    if key in self.inverted_index[w]:
                        self.inverted_index[w].discard(key)
                        # decrement term doc freq
                        if w in self.term_doc_freqs:
                            self.term_doc_freqs[w] = max(0, self.term_doc_freqs[w] - 1)
                            if self.term_doc_freqs[w] == 0:
                                del self.term_doc_freqs[w]
                    if not self.inverted_index[w]:
                        del self.inverted_index[w]
            # remove tracked words and per-doc termfreqs/lengths
            del self.key_to_words[key]
        if key in self.doc_term_freqs:
            del self.doc_term_freqs[key]
        if key in self.doc_lengths:
            del self.doc_lengths[key]
        
        # Remove embeddings
        if key in self.embeddings:
            del self.embeddings[key]

    def _index_data(self, key, val):
        # Inverted Index for Full Text Search
        words = re.findall(r'\w+', str(val).lower())
        # term frequencies for this document
        tf = {}
        for w in words:
            tf[w] = tf.get(w, 0) + 1
        total_terms = len(words)
        self.doc_term_freqs[key] = tf
        self.doc_lengths[key] = total_terms
        self.key_to_words[key] = set(tf.keys())
        for w in tf.keys():
            existed = key in self.inverted_index.get(w, set())
            self.inverted_index.setdefault(w, set()).add(key)
            if not existed:
                self.term_doc_freqs[w] = self.term_doc_freqs.get(w, 0) + 1
        
        # Embedding: use pluggable function when available, else mock deterministic per-key RNG
        if compute_embedding_fn:
            try:
                vec = compute_embedding_fn(val, key=key)
                if isinstance(vec, (list, tuple)):
                    self.embeddings[key] = list(vec)
                else:
                    # fallback to mock
                    rng = random.Random(hash(key))
                    self.embeddings[key] = [rng.random() for _ in range(128)]
            except Exception:
                rng = random.Random(hash(key))
                self.embeddings[key] = [rng.random() for _ in range(128)]
        else:
            rng = random.Random(hash(key))
            self.embeddings[key] = [rng.random() for _ in range(128)]

    def _write_wal(self, entry, debug=False):
        if debug and random.random() < 0.05:
            return False

        payload = json.dumps(entry).encode()
        framed = len(payload).to_bytes(4, "big") + payload

        flags = os.O_WRONLY | os.O_CREAT | getattr(os, 'O_APPEND', 0)
        try:
            fd = os.open(self.db_file, flags, 0o644)
            try:
                # ensure full write in a loop to avoid partial writes
                total = len(framed)
                written = 0
                while written < total:
                    n = os.write(fd, framed[written:])
                    if n is None or n == 0:
                        raise OSError("write returned 0")
                    written += n
                os.fsync(fd)
            finally:
                os.close(fd)
        except Exception:
            return False

        # fsync parent directory to persist file metadata
        try:
            dirpath = os.path.dirname(self.db_file) or '.'
            dfd = os.open(dirpath, os.O_RDONLY)
            try:
                os.fsync(dfd)
            finally:
                os.close(dfd)
        except Exception:
            pass

        return True

    async def _replicate_to_secondaries(self, entry, timeout=0.5):
        """Replicate to secondaries with quorum check"""
        # send REPL_APPEND to all peers that are not self
        acks = 0
        total = 0
        tasks = []
        
        for h, p in self.peers:
            if p == self.port and h == self.host:
                continue
            total += 1
            tasks.append(self._replicate_to_one(h, p, entry, timeout))
        
        if tasks:
            results = await asyncio.gather(*tasks, return_exceptions=True)
            acks = sum(1 for r in results if r is True)
        
        return acks, total

    async def _replicate_to_one(self, host, port, entry, timeout):
        """Replicate to a single secondary"""
        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(host, port),
                timeout=timeout
            )
            payload = json.dumps({"cmd": "REPL_APPEND", "entry": entry, "sender_port": self.port}).encode()
            writer.write(len(payload).to_bytes(4, "big") + payload)
            await writer.drain()
            
            # read framed response
            header = await asyncio.wait_for(reader.readexactly(4), timeout=timeout)
            length = int.from_bytes(header, "big")
            raw = await asyncio.wait_for(reader.readexactly(length), timeout=timeout)
            
            writer.close()
            await writer.wait_closed()
            
            if raw:
                resp = json.loads(raw.decode())
                return resp.get('status') == 'ok'
            return False
        except Exception:
            return False
    
    async def _send_heartbeat(self, h, p):
        try:
            reader, writer = await asyncio.wait_for(asyncio.open_connection(h, p), timeout=1.0)
            payload = json.dumps({"cmd": "HEARTBEAT"}).encode()
            writer.write(len(payload).to_bytes(4, "big") + payload)
            await asyncio.wait_for(writer.drain(), timeout=1.0)

            # read response
            header = await asyncio.wait_for(reader.readexactly(4), timeout=1.0)
            length = int.from_bytes(header, "big")
            response_bytes = await asyncio.wait_for(reader.readexactly(length), timeout=1.0)
            response = json.loads(response_bytes.decode())
            writer.close()
            await writer.wait_closed()
            return True
        except Exception:
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass
            return False

    # In leaderless mode we don't perform leader election; nodes accept writes and replicate

    async def handle_client(self, reader, writer):
        try:
            # read 4-byte length prefix then payload
            header = await reader.readexactly(4)
            length = int.from_bytes(header, "big")
            raw = await reader.readexactly(length)
            if not raw:
                return
            req = json.loads(raw.decode())
            cmd = req.get('cmd')
            resp = {"status": "ok"}

            if cmd in ["SET", "BULK"]:
                # Leaderless: accept writes on any node. Tag entry with vector-clock for conflict resolution.
                if '_vc' not in req or not isinstance(req.get('_vc'), dict):
                    # increment local counter
                    self.seq += 1
                    req['_vc'] = {str(self.port): self.seq}
                if self._write_wal(req, req.get('debug', False)):
                    self._apply_to_memory(req)
                    # replicate to peers and require quorum
                    acks, total = await self._replicate_to_secondaries(req)
                    quorum = (total + 2) // 2
                    if acks + 1 >= quorum:
                        resp = {"status": "ok", "acks": acks, "total": total}
                    else:
                        resp = {"status": "error", "why": "quorum_not_met", "acks": acks, "total": total}
                else:
                    resp = {"status": "error", "why": "wal_failed"}
            elif cmd == "DEL":
                if '_vc' not in req or not isinstance(req.get('_vc'), dict):
                    self.seq += 1
                    req['_vc'] = {str(self.port): self.seq}
                if self._write_wal(req, req.get('debug', False)):
                    self._apply_to_memory(req)
                    acks, total = await self._replicate_to_secondaries(req)
                    quorum = (total + 2) // 2
                    if acks + 1 >= quorum:
                        resp = {"status": "ok", "acks": acks, "total": total}
                    else:
                        resp = {"status": "error", "why": "quorum_not_met", "acks": acks, "total": total}
                else:
                    resp = {"status": "error", "why": "wal_failed"}
            elif cmd == "REPL_APPEND":
                entry = req.get('entry')
                if entry:
                    ok = self._write_wal(entry, False)
                    if ok:
                        self._apply_to_memory(entry)
                        resp = {"status": "ok"}
                    else:
                        resp = {"status": "error", "why": "wal_failed"}
            elif cmd == "ANNOUNCE":
                # ANNOUNCE is a no-op in leaderless mode
                resp = {"status": "ok"}
            elif cmd == "GET":
                val = self.data.get(req['key'])
                resp["val"] = val
                # include meta if available
                if req['key'] in self.meta:
                    resp["meta"] = self.meta[req['key']]
            elif cmd == "SEARCH":
                # Enhanced full-text search:
                # - support OR ("term1 OR term2" or using |)
                # - support phrase queries using double quotes: "exact phrase"
                # - rank results using TF-IDF
                q_raw = req.get('query', '') or ''
                q_raw = str(q_raw)
                # extract phrases
                phrases = re.findall(r'"([^"]+)"', q_raw)
                # remove phrases from query text
                q_no_phrases = re.sub(r'"([^"]+)"', ' ', q_raw)

                # split on OR (case-insensitive) or '|' to form clauses. Within a clause terms are ANDed.
                clauses = re.split(r'(?i)\s+or\s+|\|', q_no_phrases)
                clause_results = []
                all_query_terms = []
                for cl in clauses:
                    tokens = re.findall(r'\w+', cl.lower())
                    if not tokens:
                        continue
                    all_query_terms.extend(tokens)
                    sets = [self.inverted_index.get(t, set()) for t in tokens]
                    if not sets:
                        continue
                    # AND within clause
                    res = set.intersection(*sets) if len(sets) > 1 else sets[0].copy()
                    clause_results.append(res)

                # If there were clauses, union their results (OR between clauses). Otherwise empty.
                if clause_results:
                    candidate_keys = set().union(*clause_results)
                else:
                    candidate_keys = set()

                # Apply phrase filtering: require each phrase to appear as substring in the document value
                if phrases and candidate_keys:
                    filtered = set()
                    for k in candidate_keys:
                        doc_text = str(self.data.get(k, '')).lower()
                        ok = True
                        for ph in phrases:
                            if ph.lower() not in doc_text:
                                ok = False
                                break
                        if ok:
                            filtered.add(k)
                    candidate_keys = filtered

                # Ranking: compute TF-IDF score for query terms (including phrase words)
                # include words from phrases into query terms
                for ph in phrases:
                    all_query_terms.extend(re.findall(r'\w+', ph.lower()))

                # dedupe query terms
                q_terms = list(dict.fromkeys(all_query_terms))
                N = max(1, len(self.data))
                scores = {}
                for k in candidate_keys:
                    score = 0.0
                    for term in q_terms:
                        df = self.term_doc_freqs.get(term, 0)
                        idf = math.log((N + 1) / (1 + df)) + 1.0
                        tf = 0.0
                        if k in self.doc_term_freqs:
                            denom = self.doc_lengths.get(k, 0) or 1
                            tf = self.doc_term_freqs[k].get(term, 0) / denom
                        score += tf * idf
                    # phrase boost
                    if phrases:
                        # give a small boost per matched phrase
                        for ph in phrases:
                            if ph.lower() in str(self.data.get(k, '')).lower():
                                score += 0.5
                    scores[k] = score

                # sort keys by score desc
                sorted_keys = sorted(candidate_keys, key=lambda k: scores.get(k, 0.0), reverse=True)
                resp['keys'] = list(sorted_keys)
                # include scores aligned with keys for client-side ranking info
                resp['scores'] = [scores.get(k, 0.0) for k in sorted_keys]
                # build excerpts/snippets with highlighted matches
                excerpts = []
                # prepare simple case-insensitive highlight replacer
                def highlight(text, matches):
                    t = str(text)
                    # find and replace matches with <em>...</em>, avoid overlapping issues by lower-casing
                    low = t.lower()
                    spans = []
                    for m in matches:
                        mm = str(m).lower()
                        idx = low.find(mm)
                        if idx >= 0:
                            spans.append((idx, idx + len(mm)))
                    if not spans:
                        return t[:200]
                    # merge spans
                    spans.sort()
                    merged = [spans[0]]
                    for s in spans[1:]:
                        if s[0] <= merged[-1][1]:
                            merged[-1] = (merged[-1][0], max(merged[-1][1], s[1]))
                        else:
                            merged.append(s)
                    out = []
                    last = 0
                    for a, b in merged:
                        out.append(t[last:a])
                        out.append("<em>" + t[a:b] + "</em>")
                        last = b
                    out.append(t[last: last + 200])
                    return ''.join(out)

                for k in sorted_keys:
                    doc_text = str(self.data.get(k, ''))
                    matches = []
                    # prefer phrases
                    if phrases:
                        for ph in phrases:
                            matches.append(ph)
                    # add query terms
                    matches.extend(q_terms)
                    excerpt = highlight(doc_text, matches)
                    excerpts.append(excerpt)
                resp['excerpts'] = excerpts
            elif cmd == "SIMILAR":
                # find top-k similar keys to given key using cosine similarity
                key = req.get('key')
                topk = int(req.get('k', 5))
                if key not in self.embeddings:
                    resp['keys'] = []
                else:
                    v = self.embeddings[key]
                    def dot(a,b):
                        return sum(x*y for x,y in zip(a,b))
                    def norm(a):
                        return sum(x*x for x in a) ** 0.5
                    nv = norm(v) or 1.0
                    scores = []
                    for k2, vec in self.embeddings.items():
                        if k2 == key: continue
                        s = dot(v, vec) / (nv * (norm(vec) or 1.0))
                        scores.append((s, k2))
                    scores.sort(reverse=True)
                    resp['keys'] = [k for _, k in scores[:topk]]
            elif cmd == "EMBED_VALUE":
                # compute embedding for arbitrary value
                val = req.get('value')
                if compute_embedding_fn:
                    try:
                        vec = compute_embedding_fn(val)
                        resp['embedding'] = vec
                    except Exception:
                        resp['embedding'] = None
                else:
                    # deterministic mock for value
                    rng = random.Random(hash(str(val)))
                    resp['embedding'] = [rng.random() for _ in range(128)]
            elif cmd == "HEARTBEAT":
                resp = {"status": "ok", "port": self.port}
            elif cmd == "ROLE":
                resp = {"status": "ok", "port": self.port, "cluster_size": len(self.peers) + 1}

            out = json.dumps(resp, default=list).encode()
            writer.write(len(out).to_bytes(4, "big") + out)
            await writer.drain()
        finally:
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass

    async def run(self):
        while True:
            try:
                server = await asyncio.start_server(
                    self.handle_client,
                    self.host,
                    self.port,
                    reuse_address=True,
                    reuse_port=True
                )
                break
            except OSError as e:
                if e.errno == 48:
                    await asyncio.sleep(0.05)  # backoff
                else:
                    raise
        print(f"Node started on {self.port} (leaderless mode, peers={len(self.peers)})")

        try:
            async with server:
                await server.serve_forever()
        except asyncio.CancelledError:
            pass
        finally:
            server.close()
            await server.wait_closed()


if __name__ == "__main__":
    # Usage: python server.py <port> <db_file> [peer_ports_csv]
    port = int(sys.argv[1])
    db = sys.argv[2]
    peers = []
    if len(sys.argv) > 3:
        ports = [int(x) for x in sys.argv[3].split(',') if x]
        for p in ports:
            peers.append(("127.0.0.1", p))
    node = TinyDBNode("127.0.0.1", port, db, peers=peers)
    asyncio.run(node.run())