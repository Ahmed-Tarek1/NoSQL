import asyncio, json, os, random, re, sys

class TinyDBNode:
    def __init__(self, host, port, db_file):
        self.host, self.port, self.db_file = host, port, db_file
        self.data = {}
        self.inverted_index = {} 
        self.embeddings = {}     
        self._load_from_wal()

    def _load_from_wal(self):
        if not os.path.exists(self.db_file): return
        with open(self.db_file, "r") as f:
            for line in f:
                try:
                    entry = json.loads(line)
                    self._apply_to_memory(entry)
                except: continue

    def _apply_to_memory(self, entry):
        cmd = entry.get('cmd')
        if cmd == "SET":
            key, val = entry['key'], entry['val']
            self.data[key] = val
            self._index_data(key, val)
        elif cmd == "DEL":
            key = entry['key']
            if key in self.data:
                del self.data[key]
            # remove from indexes
            for w, s in list(self.inverted_index.items()):
                if key in s:
                    s.discard(key)
                    if not s: del self.inverted_index[w]
            if key in self.embeddings:
                del self.embeddings[key]
        elif cmd == "BULK":
            for k, v in entry['data']:
                self.data[k] = v
                self._index_data(k, v)

    def _index_data(self, key, val):
        # Inverted Index for Full Text Search
        words = re.findall(r'\w+', str(val).lower())
        for w in words:
            self.inverted_index.setdefault(w, set()).add(key)
        # Mock Embedding for Similarity Search
        self.embeddings[key] = [random.random() for _ in range(4)]

    def _write_wal(self, entry, debug=False):
        if debug and random.random() < 0.05: return False 
        line = json.dumps(entry) + "\n"
        with open(self.db_file, "a") as f:
            f.write(line)
            f.flush()
            os.fsync(f.fileno()) 
        return True

    async def handle_client(self, reader, writer):
        try:
            raw = await reader.read(8192)
            if not raw: return
            req = json.loads(raw.decode())
            cmd = req.get('cmd')
            resp = {"status": "ok"}

            if cmd in ["SET", "BULK"]:
                if self._write_wal(req, req.get('debug', False)):
                    self._apply_to_memory(req)
                else: resp = {"status": "error"}
            elif cmd == "DEL":
                if self._write_wal(req, req.get('debug', False)):
                    self._apply_to_memory(req)
                else:
                    resp = {"status": "error"}
            elif cmd == "GET":
                resp["val"] = self.data.get(req['key'])
            
            writer.write(json.dumps(resp, default=list).encode())
            await writer.drain()
        finally:
            writer.close()
            await writer.wait_closed()

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
        print(f"Node started on {self.port}")

        try:
            async with server:
                await server.serve_forever()
        except asyncio.CancelledError:
            pass
        finally:
            server.close()
            await server.wait_closed()


if __name__ == "__main__":
    # Usage: python server.py <port> <db_file>
    node = TinyDBNode("127.0.0.1", int(sys.argv[1]), sys.argv[2])
    asyncio.run(node.run())