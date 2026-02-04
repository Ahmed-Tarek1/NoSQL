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
        # Query all nodes and pick the latest value according to returned meta
        best = (None, None)  # (meta, val)
        for n in self.nodes:
            r = self._request(n, {"cmd": "GET", "key": key})
            if not r:
                continue
            val = r.get('val')
            meta = r.get('meta')
            if meta is None and val is None:
                continue
            if meta is None:
                # treat unknown meta as very old
                meta = {'ts': 0, 'node': 0, 'seq': 0}
            if best[0] is None:
                best = (meta, val)
                continue
            # compare by timestamp, then node, then seq
            if (meta.get('ts', 0), meta.get('node', 0), meta.get('seq', 0)) > (best[0].get('ts', 0), best[0].get('node', 0), best[0].get('seq', 0)):
                best = (meta, val)
        return best[1]

    def Delete(self, key, debug=False, verify=None):
        """Delete a key. `verify` overrides client verify_writes when provided."""
        payload = {"cmd": "DEL", "key": key, "debug": debug}
        return self._write_with_retry(payload, verify=verify)

    def BulkSet(self, pairs, debug=False, verify=None):
        """Bulk set multiple key-value pairs. `verify` overrides client verify_writes when provided."""
        payload = {"cmd": "BULK", "data": pairs, "debug": debug}
        return self._write_with_retry(payload, verify=verify)