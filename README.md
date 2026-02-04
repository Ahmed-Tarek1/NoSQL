Tiny NoSQL — Leaderless WAL-backed Key-Value Store
====================================

Overview
--------
Tiny NoSQL is a small, educational, TCP-based key-value store with:

- Persistent WAL-backed storage (framed records + fsync + parent-dir fsync)
- Leaderless replication with per-entry vector clocks, quorum writes, and read-repair
- Basic Set/Get/Del/Bulk operations exposed over a framed JSON TCP protocol
- In-memory inverted index with TF-IDF ranking, OR and phrase query support
- Pluggable embeddings (example provided using sentence-transformers) and similarity queries
- Comprehensive test harness covering durability, concurrency, replication, and search

This project is intended as a learning/demo system — not production hardened.

Quick start
-----------
1. Create a Python virtualenv and install dependencies (optional for embedding features):

```bash
python3 -m venv .venv
source .venv/bin/activate
python3 -m pip install -r requirements.txt
```

If you don't want to use heavy ML deps, you can skip installing `sentence-transformers`; the server falls back to a deterministic mock embedding generator.

2. Run a single node:

```bash
python3 server.py 5000 data0.log
```

3. Use the included synchronous client from `client.py` (example REPL):

```python
from client import DBClient
c = DBClient([("127.0.0.1", 5000)])
c.Set('foo', 'hello world')
print(c.Get('foo'))
print(c.Search('hello world'))  # returns list of (key, score, excerpt)
```

Cluster (3-node) quick start
----------------------------
Start three nodes (each node receives a comma-separated list of peer ports):

```bash
python3 server.py 6000 cluster0.log 6001,6002
python3 server.py 6001 cluster1.log 6000,6002
python3 server.py 6002 cluster2.log 6000,6001
```

Then use `DBClient` pointed at all three ports. Writes are accepted by any node; successful writes require quorum (majority) acks.

Protocol
--------
All RPCs are framed JSON messages over TCP with a 4-byte big-endian length prefix. Example commands:

- `{"cmd": "SET", "key": K, "val": V}`
- `{"cmd": "GET", "key": K}` → returns `{ "val": V, "meta": {...} }`
- `{"cmd": "DEL", "key": K}`
- `{"cmd": "BULK", "data": [[k1,v1],[k2,v2]]}`
- `{"cmd": "SEARCH", "query": Q}` → returns `{ "keys": [...], "scores": [...], "excerpts": [...] }`
- `{"cmd": "SIMILAR", "key": K, "k": 5}`
- `{"cmd": "EMBED_VALUE", "value": "text"}`

Search semantics
----------------
- Supports boolean OR (e.g. `apple OR orange` or `apple | orange`) and clause-level AND.
- Phrase queries with double quotes: `"exact phrase"` (case-insensitive substring match).
- Results are ranked by a TF-IDF score and returned as `(key, score, excerpt)` where `excerpt` contains matched text with `<em>...</em>` highlights.
- Note: excerpts are currently returned as simple HTML fragments. Clients should treat excerpts as HTML or sanitize/escape them before embedding in user-facing pages.

Embeddings
----------
- `embeddings.py` provides an example `compute_embedding(value, key=None)` implemented with `sentence-transformers` (`all-MiniLM-L6-v2`).
- If `sentence-transformers` is not installed or the model cannot be loaded, the server falls back to a deterministic pseudo-random embedding (suitable for testing).
- The model can be heavy (PyTorch). For CI or lightweight testing, skip installing `sentence-transformers`.

Running tests
-------------
The repository includes `tests.py`, a test harness that starts server subprocesses and runs durability, replication, concurrency, and search tests.

```bash
python3 -u tests.py
```

Notes & recommendations
-----------------------
- Safety: excerpts are not HTML-escaped — they include `<em>` tags for highlighting. If you display excerpts in a web UI, sanitize with `html.escape` or similar.
- WAL growth: WAL compaction/snapshots are not yet implemented; the WAL grows indefinitely — consider snapshotting to bound recovery time.
- Anti-entropy: For production, add anti-entropy (Merkle trees/gossip) to reconcile state efficiently across partitions.
- Deletes: `DEL` removes keys immediately; consider tombstones if you add anti-entropy to prevent resurrection.
- Performance: The server is single-process asyncio and keeps all indexes in memory. For large datasets consider persistent indexes and background compaction.

Developer notes
---------------
- Server entrypoint: `server.py` — Args: `<port> <db_file> [peers_csv]`.
- Client: `DBClient` in `client.py` — simple synchronous socket client.
- Tests: `tests.py` spins up processes and asserts behaviors; helpful for local experimentation.

Next improvements (tracked TODO)
-------------------------------
- Anti-entropy (Merkle trees)
- WAL compaction and snapshots
- Hinted handoff & retry
- Background gossip and metrics
- Optional CRDT merge hooks
- Partition / convergence tests

License & attribution
---------------------
This code is provided as-is for educational/demo purposes. No warranty; adapt and improve to suit your needs.

Contact / help
--------------
If you want, I can:
- Improve snippet highlighting and HTML-escape excerpts
- Implement snapshotting and WAL compaction
- Add Merkle-tree based anti-entropy and reconciliation tests

Enjoy — tell me which improvement you'd like next.
