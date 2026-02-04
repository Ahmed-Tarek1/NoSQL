import asyncio, json, os, random, re, sys
from collections import defaultdict

class TinyDBNode:
    def __init__(self, host, port, db_file, peers=None):
        self.host, self.port, self.db_file = host, port, db_file
        # peers: list of (host, port)
        self.peers = peers or []
        # role: 'primary' or 'secondary'
        self.role = 'secondary'
        # leader port if known
        self.leader_port = None
        # term number for split-brain prevention
        self.term = 0
        self.failure_count = defaultdict(int)
        # initialize leader deterministically (lowest port)
        try:
            all_ports = [self.port] + [p for _, p in self.peers]
            self.leader_port = min(all_ports)
            self.role = 'primary' if self.leader_port == self.port else 'secondary'
        except Exception:
            self.leader_port = self.port
            self.role = 'primary'
        self.data = {}
        self.inverted_index = {} 
        self.embeddings = {}
        # Track which words belong to which keys for proper cleanup
        self.key_to_words = {}
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
            # Clean up old indexes for this key if it exists
            self._cleanup_indexes(key)
            self.data[key] = val
            self._index_data(key, val)
        elif cmd == "DEL":
            key = entry['key']
            if key in self.data:
                del self.data[key]
            # remove from indexes properly
            self._cleanup_indexes(key)
        elif cmd == "BULK":
            for k, v in entry['data']:
                self._cleanup_indexes(k)
                self.data[k] = v
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
        if debug and random.random() < 0.05: return False 
        line = json.dumps(entry) + "\n"
        with open(self.db_file, "a") as f:
            f.write(line)
            f.flush()
            os.fsync(f.fileno()) 
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
            payload = json.dumps({"cmd": "REPL_APPEND", "entry": entry, "term": self.term, "leader_port": self.leader_port, "sender_port": self.port}).encode()
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
            self.failure_count[p] = 0
            return True
        except Exception:
            self.failure_count[p] += 1
            writer_close = locals().get('writer')
            if writer_close:
                writer_close.close()
                await writer_close.wait_closed()
            return self.failure_count[p] < 3  # tolerate 3 consecutive failures

    async def _get_alive_ports(self):
        alive = []

        # ✅ Self is always alive
        alive.append(self.port)

        for h, p in self.peers:
            try:
                reader, writer = await asyncio.wait_for(
                    asyncio.open_connection(h, p), timeout=0.3
                )
                # Use framed format: 4-byte length prefix + JSON
                payload = json.dumps({"cmd": "HEARTBEAT"}).encode()
                writer.write(len(payload).to_bytes(4, "big") + payload)
                await writer.drain()
                # Read framed response
                header = await asyncio.wait_for(reader.readexactly(4), timeout=0.3)
                length = int.from_bytes(header, "big")
                await asyncio.wait_for(reader.readexactly(length), timeout=0.3)
                writer.close()
                await writer.wait_closed()
                alive.append(p)
            except Exception:
                continue

        return alive
    async def _leader_alive(self):
        if self.leader_port is None:
            return False

        # ✅ If I'm the leader, I'm alive
        if self.leader_port == self.port:
            return True

        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(self.host, self.leader_port),
                timeout=0.3
            )
            # Use framed format: 4-byte length prefix + JSON
            payload = json.dumps({"cmd": "HEARTBEAT"}).encode()
            writer.write(len(payload).to_bytes(4, "big") + payload)
            await writer.drain()
            # Read framed response
            header = await asyncio.wait_for(reader.readexactly(4), timeout=0.3)
            length = int.from_bytes(header, "big")
            await asyncio.wait_for(reader.readexactly(length), timeout=0.3)
            writer.close()
            await writer.wait_closed()
            return True
        except Exception:
            return False
    
    async def _election_loop(self):
        while True:
            leader_alive = await self._leader_alive()

            # 🔥 Trigger election ONLY if leader is dead
            if not leader_alive:
                alive = await self._get_alive_ports()

                if alive:
                    new_leader = min(alive)

                    if new_leader != self.leader_port:
                        old_role = self.role
                        self.leader_port = new_leader
                        self.role = (
                            "primary" if self.port == new_leader else "secondary"
                        )
                        # If we just became primary, bump term and announce to peers
                        if self.role == "primary" and old_role != "primary":
                            self.term += 1
                            asyncio.create_task(self._announce_leader())
                        print(
                            f"[ELECTION] New leader elected: {self.leader_port} "
                            f"(I am {self.role}, term={self.term})"
                        )

            await asyncio.sleep(0.5)

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
                # only primary accepts client writes
                if self.role != 'primary':
                    resp = {"status": "error", "why": "not_primary", "leader_port": self.leader_port}
                else:
                    if self._write_wal(req, req.get('debug', False)):
                        self._apply_to_memory(req)
                        # replicate to secondaries with quorum check
                        acks, total = await self._replicate_to_secondaries(req)
                        # For single-node, total=0, quorum should be 1
                        # For 3-node, total=2, quorum should be 2 (majority of 3)
                        quorum = (total + 2) // 2
                        if acks + 1 >= quorum:  # +1 for self
                            resp = {"status": "ok", "acks": acks, "total": total}
                        else:
                            resp = {"status": "error", "why": "quorum_not_met", "acks": acks, "total": total}
                    else:
                        resp = {"status": "error", "why": "wal_failed"}
            elif cmd == "DEL":
                # allow deletes only on primary for client requests
                if self.role != 'primary':
                    resp = {"status": "error", "why": "not_primary", "leader_port": self.leader_port}
                else:
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
                term = req.get('term', 0)
                incoming_leader = req.get('leader_port')
                if entry:
                    # Accept replication from higher or equal term
                    if term >= self.term:
                        ok = self._write_wal(entry, False)
                        if ok:
                            self._apply_to_memory(entry)
                            # Update leader info from replication message
                            if incoming_leader:
                                self.leader_port = incoming_leader
                                self.role = 'primary' if self.port == self.leader_port else 'secondary'
                            self.term = term
                            resp = {"status": "ok"}
                        else:
                            resp = {"status": "error", "why": "wal_failed"}
                    else:
                        resp = {"status": "error", "why": "stale_term"}
            elif cmd == "ANNOUNCE":
                incoming_leader = req.get('leader_port')
                term = req.get('term', 0)
                if incoming_leader:
                    self.leader_port = incoming_leader
                    self.term = term
                    self.role = 'primary' if self.port == self.leader_port else 'secondary'
                resp = {"status": "ok"}
            elif cmd == "GET":
                resp["val"] = self.data.get(req['key'])
            elif cmd == "HEARTBEAT":
                resp = {"status": "ok", "role": self.role, "port": self.port, "term": self.term}
            elif cmd == "ROLE":
                resp = {"status": "ok", "role": self.role, "leader_port": self.leader_port, "term": self.term}

            out = json.dumps(resp, default=list).encode()
            writer.write(len(out).to_bytes(4, "big") + out)
            await writer.drain()
        finally:
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass

    async def _send_announce(self, host, port, timeout=0.5):
        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(host, port),
                timeout=timeout
            )
            payload = json.dumps({"cmd": "ANNOUNCE", "leader_port": self.leader_port, "term": self.term, "sender_port": self.port}).encode()
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
    
    async def _announce_leader(self):
        tasks = []
        for h, p in self.peers:
            if h == self.host and p == self.port:
                continue
            tasks.append(self._send_announce(h, p))
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

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
        print(f"Node started on {self.port} (role={self.role}, term={self.term})")
        # start election loop
        loop = asyncio.get_event_loop()
        loop.create_task(self._election_loop())

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