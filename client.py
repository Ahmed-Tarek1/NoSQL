import socket, json, time, threading

class DBClient:
    def __init__(self, nodes=None):
        # nodes: list of (host, port). If None, default to single local node 5000
        self.nodes = nodes or [("127.0.0.1", 5000)]
        self._primary_cache = None
        self._cache_time = 0
        self._cache_ttl = 1.0  # Cache primary for 1 second

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

    def find_primary(self, force_refresh=False):
        """Find the current primary node with caching"""
        now = time.time()
        
        # Use cached primary if still valid
        if not force_refresh and self._primary_cache and (now - self._cache_time) < self._cache_ttl:
            # Verify cached primary is still valid
            r = self._request(self._primary_cache, {"cmd": "ROLE"})
            if r and r.get('role') == 'primary':
                return self._primary_cache
        
        # Find primary by querying all nodes
        for n in self.nodes:
            r = self._request(n, {"cmd": "ROLE"})
            if r and r.get('role') == 'primary':
                self._primary_cache = n
                self._cache_time = now
                return n
            # If node reports a different leader, try that
            if r and r.get('leader_port'):
                leader = (n[0], r.get('leader_port'))
                if leader in self.nodes:
                    r2 = self._request(leader, {"cmd": "ROLE"})
                    if r2 and r2.get('role') == 'primary':
                        self._primary_cache = leader
                        self._cache_time = now
                        return leader
        
        # No primary found
        self._primary_cache = None
        return None

    def _write_with_retry(self, payload, max_retries=3):
        """Execute write operation with automatic retry on primary change"""
        for attempt in range(max_retries):
            primary = self.find_primary(force_refresh=(attempt > 0))
            if not primary:
                time.sleep(0.1 * (attempt + 1))
                continue
            
            r = self._request(primary, payload)
            
            if r and r.get('status') == 'ok':
                return True
            
            # Handle redirect to new primary
            if r and r.get('why') == 'not_primary':
                self._primary_cache = None  # Invalidate cache
                if r.get('leader_port'):
                    # Try to find the new leader
                    for node in self.nodes:
                        if node[1] == r.get('leader_port'):
                            r2 = self._request(node, payload)
                            if r2 and r2.get('status') == 'ok':
                                self._primary_cache = node
                                self._cache_time = time.time()
                                return True
            
            # Handle quorum failure - might succeed on retry
            if r and r.get('why') == 'quorum_not_met':
                time.sleep(0.1 * (attempt + 1))
                continue
            
            # Other errors
            if r and r.get('status') == 'error':
                break
        
        return False

    def Set(self, key, value, debug=False):
        """Set a key-value pair"""
        payload = {"cmd": "SET", "key": key, "val": value, "debug": debug}
        return self._write_with_retry(payload)

    def Get(self, key):
        """Get a value by key - tries all nodes for availability"""
        # Try primary first
        primary = self.find_primary()
        if primary:
            r = self._request(primary, {"cmd": "GET", "key": key})
            if r and 'val' in r:
                return r['val']
        
        # Fallback to any available node
        for n in self.nodes:
            if n == primary:
                continue
            r = self._request(n, {"cmd": "GET", "key": key})
            if r and 'val' in r:
                return r['val']
        
        return None

    def Delete(self, key, debug=False):
        """Delete a key"""
        payload = {"cmd": "DEL", "key": key, "debug": debug}
        return self._write_with_retry(payload)

    def BulkSet(self, pairs, debug=False):
        """Bulk set multiple key-value pairs"""
        payload = {"cmd": "BULK", "data": pairs, "debug": debug}
        return self._write_with_retry(payload)