import socket, json, time, threading

class DBClient:
    def __init__(self, nodes=None):
        # nodes: list of (host, port). If None, default to single local node 5000
        self.nodes = nodes or [("127.0.0.1", 5000)]

    def _recv_all(self, sock):
        parts = []
        sock.settimeout(1.0)
        try:
            while True:
                data = sock.recv(16384)
                if not data: break
                parts.append(data)
                # if less than buffer, probably done
                if len(data) < 16384:
                    break
        except socket.timeout:
            pass
        return b"".join(parts)

    def _request(self, node, payload, timeout=1.0):
        try:
            with socket.create_connection(node, timeout=timeout) as s:
                s.sendall(json.dumps(payload).encode())
                raw = self._recv_all(s)
                if not raw: return None
                return json.loads(raw.decode())
        except Exception:
            return None

    def Set(self, key, value, debug=False):
        # write to all configured nodes, return True if majority ack
        acks = 0
        for n in self.nodes:
            r = self._request(n, {"cmd": "SET", "key": key, "val": value, "debug": debug})
            if r and r.get('status') == 'ok':
                acks += 1
        return acks >= (len(self.nodes) // 2) + 1

    def Get(self, key):
        # Try nodes in order until we get a value
        for n in self.nodes:
            r = self._request(n, {"cmd": "GET", "key": key})
            if r and 'val' in r:
                return r['val']
        return None

    def Delete(self, key, debug=False):
        acks = 0
        for n in self.nodes:
            r = self._request(n, {"cmd": "DEL", "key": key, "debug": debug})
            if r and r.get('status') == 'ok':
                acks += 1
        return acks >= (len(self.nodes) // 2) + 1

    def BulkSet(self, pairs, debug=False):
        acks = 0
        for n in self.nodes:
            r = self._request(n, {"cmd": "BULK", "data": pairs, "debug": debug})
            if r and r.get('status') == 'ok':
                acks += 1
        return acks >= (len(self.nodes) // 2) + 1