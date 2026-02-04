import os, time, subprocess, sys, threading, random, signal
from client import DBClient


def start_server(port, db_file, peers_csv=None):
    """Start a server process"""
    args = [sys.executable, "server.py", str(port), db_file]
    if peers_csv:
        args.append(peers_csv)
    return subprocess.Popen(args)


def stop_server(proc, graceful=False):
    """Stop a server process"""
    if graceful:
        proc.terminate()
    else:
        os.kill(proc.pid, signal.SIGKILL)
    proc.wait()


def test_basic_scenarios():
    """Test basic CRUD operations and persistence"""
    print("\n=== Testing Basic Scenarios ===")
    db_file = "test_basic.log"
    if os.path.exists(db_file): os.remove(db_file)
    proc = start_server(5000, db_file)
    time.sleep(0.5)
    client = DBClient([("127.0.0.1", 5000)])

    # Set then Get
    assert client.Set("a", "1"), "Failed to set key 'a'"
    assert client.Get("a") == "1", "Failed to get key 'a'"
    print("✓ Set and Get working")

    # Set then Delete then Get
    assert client.Set("b", "2"), "Failed to set key 'b'"
    assert client.Delete("b"), "Failed to delete key 'b'"
    assert client.Get("b") is None, "Key 'b' should be deleted"
    print("✓ Delete working")

    # Get without setting
    assert client.Get("nosuchkey") is None, "Non-existent key should return None"
    print("✓ Get non-existent key returns None")

    # Set then Set (same key) then Get
    assert client.Set("c", "first"), "Failed to set key 'c' first time"
    assert client.Set("c", "second"), "Failed to set key 'c' second time"
    assert client.Get("c") == "second", "Key 'c' should have latest value"
    print("✓ Overwrite working")

    # Set then exit gracefully then Get (persistence test)
    assert client.Set("d", "dur"), "Failed to set key 'd'"
    stop_server(proc, graceful=True)
    # restart
    proc = start_server(5000, db_file)
    time.sleep(0.5)
    assert client.Get("d") == "dur", "Key 'd' should persist after restart"
    print("✓ Persistence working")

    stop_server(proc, graceful=True)
    print("✅ Basic scenario tests passed")


def benchmark_throughput(client):
    """Benchmark write throughput at different database sizes"""
    print("\n=== Benchmarking Throughput ===")
    for pre in [0, 100, 1000, 5000, 10000, 50000]:
        # pre-populate
        for i in range(pre):
            client.Set(f"p_{i}", "x")
        # measure
        N = 1000
        start = time.time()
        for i in range(N):
            client.Set(f"bench_{pre}_{i}", "data")
        dur = time.time() - start
        print(f"  pre={pre:4d} keys: {N/dur:6.2f} writes/sec")


def durability_test_with_random_kill():
    """Test that acknowledged writes survive random crashes"""
    print("\n=== Testing Durability with Random Kills ===")
    db_file = "dur_kill.log"
    if os.path.exists(db_file): os.remove(db_file)
    proc = start_server(5001, db_file)
    time.sleep(0.5)
    client = DBClient([("127.0.0.1", 5001)])

    acked = []
    stop_flag = threading.Event()

    def writer():
        i = 0
        while i < 100:
            key = f"k_{i}"
            ok = client.Set(key, f"v_{i}")
            if ok:
                acked.append(key)
            i += 1
            time.sleep(0.01)
        stop_flag.set()

    # use mutable holder so killer thread can replace running process
    proc_holder = { 'proc': proc }

    def killer():
        # randomly kill and restart server while writer runs
        while not stop_flag.is_set():
            time.sleep(random.uniform(0.05, 0.5))
            try:
                os.kill(proc_holder['proc'].pid, signal.SIGKILL)
            except Exception:
                pass
            try:
                proc_holder['proc'].wait()
            except Exception:
                pass
            # restart quickly
            proc_new = start_server(5001, db_file)
            proc_holder['proc'] = proc_new
            time.sleep(0.05)

    t_writer = threading.Thread(target=writer)
    t_killer = threading.Thread(target=killer)
    t_writer.start()
    t_killer.start()
    t_writer.join()
    
    # Stop killer and ensure server is running for verification
    stop_flag.set()
    try:
        if proc_holder['proc'].poll() is not None:
            proc_holder['proc'] = start_server(5001, db_file)
            time.sleep(0.3)
    except Exception:
        proc_holder['proc'] = start_server(5001, db_file)
        time.sleep(0.3)

    # check which acked keys were lost after ensuring server is up
    lost = []
    for k in acked:
        if client.Get(k) is None:
            lost.append(k)

    print(f"  Acked: {len(acked)}, Lost after restart: {len(lost)}")
    
    # show lost keys for debugging
    if lost:
        print("  Lost keys:", lost)

    # FIXED: Add assertion to fail test on data loss
    assert len(lost) == 0, f"Durability violated: {len(lost)} acknowledged writes were lost!"
    
    # Cleanup
    try:
        stop_server(proc_holder['proc'], graceful=True)
    except Exception:
        pass
    
    print("✅ Durability test passed (no data loss)")


def test_concurrent_bulk():
    """Test concurrent bulk writes don't corrupt data"""
    print("\n=== Testing Concurrent Bulk Writes ===")
    db_file = "test_concurrent.log"
    if os.path.exists(db_file): os.remove(db_file)
    proc = start_server(5002, db_file)
    time.sleep(0.5)
    client = DBClient([("127.0.0.1", 5002)])

    def worker(val):
        client.BulkSet([("conflict_key", val), (f"other_key_{val}", val)])

    threads = [threading.Thread(target=worker, args=(f"val_{i}",)) for i in range(8)]
    for t in threads: t.start()
    for t in threads: t.join()

    final = client.Get("conflict_key")
    assert final is not None, "Concurrent writes should produce some value"
    assert final.startswith("val_"), "Final value should be from one of the workers"
    print(f"  Concurrent bulk final value: {final}")
    print("✓ No data corruption")
    
    stop_server(proc, True)
    print("✅ Concurrent bulk test passed")

def wait_for_port_closed(port, timeout=2.0):
    """Wait for a port to be fully closed"""
    import socket
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            # Try to connect - if it succeeds, port is still open
            s = socket.create_connection(('127.0.0.1', port), timeout=0.1)
            s.close()
            time.sleep(0.05)  # Port still open, wait a bit
        except (ConnectionRefusedError, OSError):
            # Port is closed!
            return True
    return False

def test_replication_and_failover():
    """Test data replication and automatic failover"""
    print("\n=== Testing Replication and Failover ===")
    
    # Start 3-node cluster: ports 6000,6001,6002
    db_files = ["cluster0.log", "cluster1.log", "cluster2.log"]
    for f in db_files:
        if os.path.exists(f): os.remove(f)

    # peers lists (each node gets list of other ports)
    ports = [6000, 6001, 6002]
    procs = []
    for i, p in enumerate(ports):
        others = [str(x) for x in ports if x != p]
        peers_csv = ",".join(others)
        proc = start_server(p, db_files[i], peers_csv)
        procs.append(proc)
    time.sleep(1.5)

    client = DBClient([("127.0.0.1", p) for p in ports])

    # write some keys (writes can go to any node in leaderless mode)
    print("  Writing 20 keys...")
    for i in range(20):
        assert client.Set(f"k{i}", f"v{i}"), f"Failed to write key k{i}"
    print("✓ All writes succeeded")

    # Verify replication: each node should have the key
    time.sleep(0.5)
    for port in ports:
        test_client = DBClient([("127.0.0.1", port)])
        val = test_client.Get("k0")
        assert val == "v0", f"Node {port} should have replicated data"
    print("✓ Data replicated to all nodes")

    # kill one node
    kill_idx = 0
    kill_port = ports[kill_idx]
    print(f"  Killing node on port {kill_port}...")
    try:
        os.kill(procs[kill_idx].pid, signal.SIGKILL)
        for _ in range(10):
            if procs[kill_idx].poll() is not None:
                break
            time.sleep(0.1)
        procs[kill_idx] = None
        print(f"  Node {kill_port} killed")
    except Exception as e:
        print(f"  Warning: Error killing node: {e}")

    # Wait for port to close
    print(f"  Waiting for port {kill_port} to close...")
    wait_for_port_closed(kill_port, timeout=3.0)

    # Verify existing data still accessible and new writes succeed (quorum of 2/3)
    print("  Verifying data integrity and new writes after node failure...")
    for i in range(20):
        val = client.Get(f"k{i}")
        assert val == f"v{i}", f"Missing k{i} after node failure"

    # New writes should still succeed with 2/3 nodes up
    for i in range(20, 25):
        assert client.Set(f"k{i}", f"v{i}"), f"Failed to write k{i} after node failure"
    print("✓ New writes successful after node failure")

    # cleanup remaining procs
    for proc in procs:
        try:
            if proc and proc.poll() is None:
                proc.terminate()
                proc.wait(timeout=2)
        except Exception:
            pass
    
    print("✅ Replication and failover test passed")


def test_conflict_and_convergence():
    """Test concurrent conflicting writes and convergence via read-repair"""
    print("\n=== Testing Conflict Resolution and Convergence ===")
    db_files = ["conf0.log", "conf1.log", "conf2.log"]
    for f in db_files:
        if os.path.exists(f): os.remove(f)
    ports = [8000, 8001, 8002]
    procs = []
    for i, p in enumerate(ports):
        peers_csv = ",".join(str(x) for x in ports if x != p)
        procs.append(start_server(p, db_files[i], peers_csv))
    time.sleep(1.0)

    # Write conflicting values to different nodes concurrently
    c1 = DBClient([("127.0.0.1", 8000)], verify_writes=False)
    c2 = DBClient([("127.0.0.1", 8001)], verify_writes=False)
    assert c1.Set("conflict", "A", verify=False)
    assert c2.Set("conflict", "B", verify=False)

    # Read from cluster and ensure a deterministic winner and that nodes converge
    cluster_client = DBClient([("127.0.0.1", p) for p in ports])
    val = cluster_client.Get("conflict")
    assert val in ("A", "B"), f"Unexpected value {val}"

    # After read, read-repair should have propagated the winner. Verify all nodes have same value
    time.sleep(0.2)
    for p in ports:
        tc = DBClient([("127.0.0.1", p)])
        v = tc.Get("conflict")
        assert v == val, f"Node {p} did not converge (got {v}, expected {val})"

    # cleanup
    for proc in procs:
        try:
            if proc and proc.poll() is None:
                proc.terminate()
                proc.wait(timeout=2)
        except Exception:
            pass

    print("✅ Conflict resolution and convergence test passed")


def test_search_and_embeddings():
    """Test inverted index search and embedding APIs"""
    print("\n=== Testing Search and Embeddings ===")
    db_files = ["idx0.log", "idx1.log", "idx2.log"]
    for f in db_files:
        if os.path.exists(f): os.remove(f)
    ports = [8100, 8101, 8102]
    procs = []
    for i, p in enumerate(ports):
        peers_csv = ",".join(str(x) for x in ports if x != p)
        procs.append(start_server(p, db_files[i], peers_csv))
    time.sleep(1.0)

    client = DBClient([("127.0.0.1", p) for p in ports], verify_writes=False)
    # add documents
    docs = {
        'doc1': 'The quick brown fox',
        'doc2': 'Quick brown dogs leap',
        'doc3': 'Lazy fox sleeps'
    }
    for k, v in docs.items():
        assert client.Set(k, v, verify=False)

    # search for 'quick brown' should return doc1 and doc2
    res = client.Search('quick brown')
    # client.Search now returns list of (key, score, excerpt)
    keys = [t[0] for t in res]
    assert 'doc1' in keys and 'doc2' in keys, f"Search missing results: {res}"

    # check that excerpts contain highlighted matches
    for k, s, excerpt in res:
        if k in ('doc1', 'doc2'):
            assert excerpt is not None and '<em>' in excerpt.lower(), f"Excerpt missing highlight for {k}: {excerpt}"

    # test embedding API returns vector
    emb = client.EmbedValue('hello world')
    assert isinstance(emb, list) and len(emb) >= 1, "EmbedValue failed"

    # test similarity: pick doc1 neighbors
    sim = client.SimilarKeys('doc1', k=2)
    assert isinstance(sim, list), "SimilarKeys failed"

    # cleanup
    for proc in procs:
        try:
            if proc and proc.poll() is None:
                proc.terminate()
                proc.wait(timeout=2)
        except Exception:
            pass

    print("✅ Search and embeddings test passed")


def test_search_edge_cases():
    """Unit tests for OR, phrase queries, and TF-IDF ranking."""
    print("\n=== Testing SEARCH Edge Cases (OR, phrase, TF-IDF) ===")
    db_file = "edge_idx.log"
    if os.path.exists(db_file): os.remove(db_file)
    proc = start_server(8200, db_file)
    time.sleep(0.5)
    client = DBClient([("127.0.0.1", 8200)], verify_writes=False)

    docs = {
        'a': 'apple apple apple banana',
        'b': 'apple banana',
        'c': 'orange fruit salad',
        'd': 'the quick brown fox jumps'
    }
    for k, v in docs.items():
        assert client.Set(k, v, verify=False)

    # OR query: 'orange OR apple' should include both 'a','b','c'
    res = client.Search('orange OR apple')
    keys = [t[0] for t in res]
    assert 'a' in keys and 'b' in keys and 'c' in keys, f"OR query failed: {keys}"

    # Phrase query: exact phrase should match
    assert any(t[0] == 'd' for t in client.Search('"quick brown"')) , "Phrase query failed"

    # TF-IDF ranking: document 'a' has higher tf for 'apple' than 'b'
    res = client.Search('apple')
    if len(res) >= 2:
        # Expect 'a' to rank above 'b'
        keys = [t[0] for t in res]
        assert keys.index('a') <= keys.index('b'), f"TF-IDF ranking unexpected: {res}"

    stop_server(proc, True)
    print("✅ SEARCH edge cases tests passed")


def test_quorum_requirement():
    """Test that writes require quorum acknowledgment"""
    print("\n=== Testing Quorum Requirement ===")
    
    # Start 3-node cluster
    db_files = ["quorum0.log", "quorum1.log", "quorum2.log"]
    for f in db_files:
        if os.path.exists(f): os.remove(f)

    ports = [7000, 7001, 7002]
    procs = []
    for i, p in enumerate(ports):
        others = [str(x) for x in ports if x != p]
        peers_csv = ",".join(others)
        proc = start_server(p, db_files[i], peers_csv)
        procs.append(proc)
    time.sleep(1.5)

    client = DBClient([("127.0.0.1", p) for p in ports])
    # Write should succeed with all nodes up (quorum: 2/3)
    assert client.Set("test_key", "test_value"), "Write should succeed with all nodes"
    print("✓ Write succeeded with full cluster")
    
    # Kill one secondary - writes should still succeed (quorum: 2/3 still possible)
    # Kill one node (any) - writes should still succeed with quorum
    killed_idx = 0
    killed_port = ports[killed_idx]
    print(f"  Killing node on port {killed_port}...")
    try:
        os.kill(procs[killed_idx].pid, signal.SIGKILL)
        for _ in range(10):
            if procs[killed_idx].poll() is not None:
                break
            time.sleep(0.1)
        procs[killed_idx] = None
    except Exception:
        pass
    
    time.sleep(0.5)
    assert client.Set("quorum_key", "quorum_value"), "Write should succeed with 2/3 nodes"
    print("✓ Write succeeded with 2/3 nodes (quorum met)")
    
    # cleanup
    for proc in procs:
        try:
            if proc and proc.poll() is None:
                proc.terminate()
                proc.wait(timeout=2)
        except Exception:
            pass
    
    print("✅ Quorum requirement test passed")


if __name__ == "__main__":
    try:
        test_basic_scenarios()
        durability_test_with_random_kill()
        test_concurrent_bulk()
        test_replication_and_failover()
        test_quorum_requirement()
    except AssertionError as e:
        print("TEST FAILED:", e)
        sys.exit(1)
    except Exception as e:
        print("ERROR while running tests:", e)
        sys.exit(2)
    print("\n" + "=" * 60)
    print("✅ ALL TESTS PASSED!")
    print("=" * 60)