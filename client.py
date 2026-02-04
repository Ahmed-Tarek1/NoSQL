import socket, json, time, threading

class DBClient:
    def __init__(self, nodes=None, verify_writes=True):
        # nodes: list of (host, port). If None, default to single local node 5000
        self.nodes = nodes or [("127.0.0.1", 5000)]
        # Whether client verifies writes with a read-after-write check
        self.verify_writes = verify_writes

    def _recv_all(self, sock):
        # Read a 4-byte big-endian length prefix then that many bytes
        def read_n(n):
            buf = b""
            sock.settimeout(1.0)
            while len(buf) < n:
                chunk = sock.recv(n - len(buf))
                if not chunk:
                    raise ConnectionError("socket closed")
                buf += chunk
            return buf

        try:
            header = read_n(4)
        except Exception:
            return b""
        length = int.from_bytes(header, "big")
        try:
            return read_n(length)
        except Exception:
            return b""

    def _request(self, node, payload, timeout=1.0):
        try:
            with socket.create_connection(node, timeout=timeout) as s:
                data = json.dumps(payload).encode()
                header = len(data).to_bytes(4, "big")
                s.sendall(header + data)
                raw = self._recv_all(s)
                if not raw:
                    return None
                return json.loads(raw.decode())
        except Exception as e:
            return None
    def _compare_vc(self, a, b):
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
    def _write_with_retry(self, payload, max_retries=3, verify=None):
        """Try to send the write to any node in the cluster until success or retries exhausted.

        If `verify` is None, falls back to `self.verify_writes`.
        """
        if verify is None:
            verify = self.verify_writes
        if not self.nodes:
            return False
        for attempt in range(max_retries):
            for node in self.nodes:
                r = self._request(node, payload)
                if r and r.get('status') == 'ok':
                    if not verify:
                        return True
                    # perform read-after-write verification to ensure durability
                    cmd = payload.get('cmd')
                    if cmd == 'SET':
                        key = payload.get('key')
                        val = payload.get('val')
                        verify_r = self._request(node, {'cmd': 'GET', 'key': key})
                        if verify_r and verify_r.get('val') == val:
                            return True
                        else:
                            continue
                    elif cmd == 'DEL':
                        key = payload.get('key')
                        verify_r = self._request(node, {'cmd': 'GET', 'key': key})
                        if verify_r and verify_r.get('val') is None:
                            return True
                        else:
                            continue
                    elif cmd == 'BULK':
                        ok_all = True
                        for k, v in payload.get('data', []):
                            verify_r = self._request(node, {'cmd': 'GET', 'key': k})
                            if not (verify_r and verify_r.get('val') == v):
                                ok_all = False
                                break
                        if ok_all:
                            return True
                        else:
                            continue
                    else:
                        return True
                # transient quorum failure - try other nodes
                if r and r.get('why') == 'quorum_not_met':
                    continue
            time.sleep(0.05 * (attempt + 1))
        return False

    def Set(self, key, value, debug=False, verify=None):
        """Set a key-value pair. `verify` overrides client verify_writes when provided."""
        payload = {"cmd": "SET", "key": key, "val": value, "debug": debug}
        return self._write_with_retry(payload, verify=verify)

    def Get(self, key):
        """Get a value by key - tries all nodes for availability"""
        # Query all nodes and pick the latest value according to vector clocks
        responses = []  # tuples (node, val, vc)
        for n in self.nodes:
            r = self._request(n, {"cmd": "GET", "key": key})
            if not r:
                continue
            val = r.get('val')
            vc = None
            meta = r.get('meta')
            if isinstance(meta, dict) and isinstance(meta.get('vc'), dict):
                vc = meta.get('vc')
            responses.append((n, val, vc))

        if not responses:
            return None

        # pick a winner
        winner = responses[0]
        for tup in responses[1:]:
            cmp = self._compare_vc(tup[2], winner[2])
            if cmp == 1:
                winner = tup
            elif cmp == 2:
                # concurrent - deterministic tie-breaker: pick max (node, seq) pair
                def max_pair(vc):
                    if not vc: return (0, 0)
                    items = [(int(n), vc[n]) for n in vc]
                    return max(items)
                wp = max_pair(winner[2])
                tp = max_pair(tup[2])
                if tp > wp:
                    winner = tup

        # read-repair: write winner back to nodes with older vc
        w_node, w_val, w_vc = winner
        for n, val, vc in responses:
            if self._compare_vc(w_vc, vc) == 1:
                # repair
                try:
                    self._request(n, {"cmd": "REPL_APPEND", "entry": {"cmd": "SET", "key": key, "val": w_val, "_vc": w_vc}})
                except Exception:
                    pass

        return winner[1]

    def Search(self, query):
        """Full-text search returning matching keys (AND semantics)."""
        for node in self.nodes:
            r = self._request(node, {"cmd": "SEARCH", "query": query})
            if r and 'keys' in r:
                return r['keys']
        return []

    def SimilarKeys(self, key, k=5):
        """Return top-k similar keys to `key` based on embeddings."""
        for node in self.nodes:
            r = self._request(node, {"cmd": "SIMILAR", "key": key, "k": k})
            if r and 'keys' in r:
                return r['keys']
        return []

    def EmbedValue(self, value):
        """Compute embedding for an arbitrary value (server-side or mock)."""
        for node in self.nodes:
            r = self._request(node, {"cmd": "EMBED_VALUE", "value": value})
            if r and 'embedding' in r:
                return r['embedding']
        return None

    def Delete(self, key, debug=False, verify=None):
        """Delete a key. `verify` overrides client verify_writes when provided."""
        payload = {"cmd": "DEL", "key": key, "debug": debug}
        return self._write_with_retry(payload, verify=verify)

    def BulkSet(self, pairs, debug=False, verify=None):
        """Bulk set multiple key-value pairs. `verify` overrides client verify_writes when provided."""
        payload = {"cmd": "BULK", "data": pairs, "debug": debug}
        return self._write_with_retry(payload, verify=verify)