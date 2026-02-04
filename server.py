import asyncio, json, os, random, re, sys, time

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

    def _apply_to_memory(self, entry):
        cmd = entry.get('cmd')
        if cmd == "SET":
            key, val = entry['key'], entry['val']
            # Clean up old indexes for this key if it exists
            self._cleanup_indexes(key)
            self.data[key] = val
            # store meta if provided
            if isinstance(entry.get('_meta'), dict):
                self.meta[key] = entry['_meta']
            self._index_data(key, val)
        elif cmd == "DEL":
            key = entry['key']
            if key in self.data:
                del self.data[key]
            if key in self.meta:
                del self.meta[key]
            # remove from indexes properly
            self._cleanup_indexes(key)
        elif cmd == "BULK":
            for k, v in entry['data']:
                self._cleanup_indexes(k)
                self.data[k] = v
                # store per-key meta if present on entry
                if isinstance(entry.get('_meta'), dict):
                    self.meta[k] = entry['_meta']
                self._index_data(k, v)

    def _cleanup_indexes(self, key):
        """Properly cleanup all indexes for a key"""
        # Remove from inverted index using tracked words
        if key in self.key_to_words:
            for w in self.key_to_words[key]:
                if w in self.inverted_index:
                    self.inverted_index[w].discard(key)
                    if not self.inverted_index[w]:
                        del self.inverted_index[w]
            del self.key_to_words[key]
        
        # Remove embeddings
        if key in self.embeddings:
            del self.embeddings[key]

    def _index_data(self, key, val):
        # Inverted Index for Full Text Search
        words = re.findall(r'\w+', str(val).lower())
        self.key_to_words[key] = set()
        for w in words:
            self.inverted_index.setdefault(w, set()).add(key)
            self.key_to_words[key].add(w)
        
        # Fixed: Mock Embedding for Similarity Search using per-key RNG
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
                # Leaderless: accept writes on any node. Tag entry with per-entry metadata for conflict resolution.
                if '_meta' not in req or not isinstance(req.get('_meta'), dict):
                    req['_meta'] = {'ts': time.time(), 'node': self.port, 'seq': self.seq}
                    self.seq += 1
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
                if '_meta' not in req or not isinstance(req.get('_meta'), dict):
                    req['_meta'] = {'ts': time.time(), 'node': self.port, 'seq': self.seq}
                    self.seq += 1
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