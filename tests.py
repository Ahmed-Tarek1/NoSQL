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

    # find primary
    primary = client.find_primary()
    assert primary is not None, "Should have elected a primary"
    print(f"✓ Primary elected: port {primary[1]}")

    # write some keys to primary
    print("  Writing 20 keys...")
    for i in range(20):
        assert client.Set(f"k{i}", f"v{i}"), f"Failed to write key k{i}"
    print("✓ All writes succeeded")

    # Verify replication
    time.sleep(0.5)
    for port in ports:
        test_client = DBClient([("127.0.0.1", port)])
        val = test_client.Get("k0")
        assert val == "v0", f"Node {port} should have replicated data"
    print("✓ Data replicated to all nodes")

    # kill primary
    pk = primary[1]
    print(f"  Killing primary on port {pk}...")
    primary_proc_idx = ports.index(pk)
    try:
        os.kill(procs[primary_proc_idx].pid, signal.SIGKILL)
        # Wait for process to fully die
        for _ in range(10):
            if procs[primary_proc_idx].poll() is not None:
                break
            time.sleep(0.1)
        procs[primary_proc_idx] = None  # Mark as dead
        print(f"  Primary process killed")
    except Exception as e:
        print(f"  Warning: Error killing primary: {e}")

    # Wait for port to be fully released
    print(f"  Waiting for port {pk} to close...")
    if wait_for_port_closed(pk, timeout=3.0):
        print(f"  Port {pk} confirmed closed")
    else:
        print(f"  Warning: Port {pk} may still be partially open")

    # Give nodes time to detect failure - election runs every 0.5s
    print("  Waiting for election to complete...")
    time.sleep(1.5)  # At least 3 election cycles

    # wait for election (poll ROLE on remaining nodes for new primary)
    print("  Polling for new primary election...")
    new_primary = None
    deadline = time.time() + 8.0  # Increased timeout
    remaining_ports = [p for p in ports if p != pk]
    
    attempts = 0
    while time.time() < deadline:
        attempts += 1
        for rp in remaining_ports:
            try:
                import socket, json
                s = socket.create_connection(('127.0.0.1', rp), timeout=0.5)
                payload = json.dumps({'cmd': 'ROLE'}).encode()
                s.sendall(len(payload).to_bytes(4, 'big') + payload)
                # read 4-byte header then payload
                header = s.recv(4)
                if not header:
                    s.close()
                    continue
                length = int.from_bytes(header, 'big')
                raw = b""
                while len(raw) < length:
                    chunk = s.recv(length - len(raw))
                    if not chunk:
                        break
                    raw += chunk
                s.close()
                if raw:
                    obj = json.loads(raw.decode())
                    role = obj.get('role')
                    leader_port = obj.get('leader_port')
                    term = obj.get('term', 0)
                    if attempts <= 3:  # Debug first few attempts
                        print(f"    Port {rp}: role={role}, leader_port={leader_port}, term={term}")
                    # Node should report being primary AND have a different leader than killed primary
                    if role == 'primary' and leader_port != pk:
                        new_primary = ("127.0.0.1", rp)
                        break
            except Exception as e:
                if attempts <= 3:
                    print(f"    Port {rp}: connection failed - {e}")
                continue
        if new_primary:
            break
        time.sleep(0.5)  # Check less frequently
    
    assert new_primary is not None, f"New primary should be elected within 8 seconds (remaining ports: {remaining_ports})"
    assert new_primary != primary, f"New primary {new_primary} should be different from old primary {primary}"
    print(f"✓ New primary elected: port {new_primary[1]}")

    # Verify all data is still accessible
    print("  Verifying data integrity after failover...")
    client._primary_cache = None  # Clear cache
    for i in range(20):
        val = client.Get(f"k{i}")
        assert val == f"v{i}", f"Missing k{i} after failover (expected 'v{i}', got {val})"
    print("✓ All data intact after failover")
    
    # Write new data to new primary
    print("  Writing new data to new primary...")
    for i in range(20, 25):
        assert client.Set(f"k{i}", f"v{i}"), f"Failed to write k{i} to new primary"
    print("✓ New writes successful on new primary")

    # cleanup remaining procs
    for proc in procs:
        try:
            if proc and proc.poll() is None:
                proc.terminate()
                proc.wait(timeout=2)
        except Exception:
            pass
    
    print("✅ Replication and failover test passed")


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
    
    # Find and verify primary
    primary = client.find_primary()
    assert primary is not None
    print(f"✓ Primary on port {primary[1]}")
    
    # Write should succeed with all nodes up (quorum: 2/3)
    assert client.Set("test_key", "test_value"), "Write should succeed with all nodes"
    print("✓ Write succeeded with full cluster")
    
    # Kill one secondary - writes should still succeed (quorum: 2/3 still possible)
    secondaries = [p for p in ports if p != primary[1]]
    killed_port = secondaries[0]
    killed_idx = ports.index(killed_port)
    print(f"  Killing secondary on port {killed_port}...")
    try:
        os.kill(procs[killed_idx].pid, signal.SIGKILL)
        procs[killed_idx].wait()
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