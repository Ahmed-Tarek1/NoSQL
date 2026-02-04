import os, time, subprocess, sys, threading, random, signal
from client import DBClient


def start_server(port, db_file):
    return subprocess.Popen([sys.executable, "server.py", str(port), db_file])


def stop_server(proc, graceful=False):
    if graceful:
        proc.terminate()
    else:
        os.kill(proc.pid, signal.SIGKILL)
    proc.wait()


def test_basic_scenarios():
    db_file = "test_basic.log"
    if os.path.exists(db_file): os.remove(db_file)
    proc = start_server(5000, db_file)
    time.sleep(0.5)
    client = DBClient([("127.0.0.1", 5000)])

    # Set then Get
    assert client.Set("a", "1")
    assert client.Get("a") == "1"

    # Set then Delete then Get
    assert client.Set("b", "2")
    assert client.Delete("b")
    assert client.Get("b") is None

    # Get without setting
    assert client.Get("nosuchkey") is None

    # Set then Set (same key) then Get
    assert client.Set("c", "first")
    assert client.Set("c", "second")
    assert client.Get("c") == "second"

    # Set then exit gracefully then Get
    assert client.Set("d", "dur")
    stop_server(proc, graceful=True)
    # restart
    proc = start_server(5000, db_file)
    time.sleep(0.5)
    assert client.Get("d") == "dur"

    stop_server(proc, graceful=True)
    print("Basic scenario tests passed")


def benchmark_throughput(client):
    print("Benchmark: write throughput vs pre-populated size")
    for pre in [0, 100, 1000, 5000]:
        # pre-populate
        for i in range(pre):
            client.Set(f"p_{i}", "x")
        # measure
        N = 1000
        start = time.time()
        for i in range(N):
            client.Set(f"bench_{pre}_{i}", "data")
        dur = time.time() - start
        print(f"pre={pre}: {N/dur:.2f} writes/sec")


def durability_test_with_random_kill():
    db_file = "dur_kill.log"
    if os.path.exists(db_file): os.remove(db_file)
    proc = start_server(5001, db_file)
    time.sleep(0.5)
    client = DBClient([("127.0.0.1", 5001)])

    acked = []
    stop_flag = threading.Event()

    def writer():
        i = 0
        while i < 500:
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
    # ensure server is running for verification; restart if needed
    try:
        if proc_holder['proc'].poll() is not None:
            proc_holder['proc'] = start_server(5001, db_file)
            time.sleep(0.2)
    except Exception:
        proc_holder['proc'] = start_server(5001, db_file)
        time.sleep(0.2)

    # check which acked keys were lost after ensuring server is up
    lost = []
    for k in acked:
        if client.Get(k) is None:
            lost.append(k)

    print(f"Acked: {len(acked)}, Lost after restart: {len(lost)}")


def test_concurrent_bulk():
    db_file = "test_concurrent.log"
    if os.path.exists(db_file): os.remove(db_file)
    proc = start_server(5002, db_file)
    time.sleep(0.5)
    client = DBClient([("127.0.0.1", 5002)])

    def worker(val):
        client.BulkSet([("conflict_key", val), ("other_key", val)])

    threads = [threading.Thread(target=worker, args=(f"val_{i}",)) for i in range(8)]
    for t in threads: t.start()
    for t in threads: t.join()

    final = client.Get("conflict_key")
    assert final is not None
    print(f"Concurrent bulk final value: {final}")
    stop_server(proc, True)


if __name__ == "__main__":
    print("Running basic scenario tests...")
    test_basic_scenarios()
    print("Running concurrency test...")
    test_concurrent_bulk()
    print("Running benchmark (may take a while)...")
    p = start_server(5000, "bench.log")
    time.sleep(0.3)
    c = DBClient([("127.0.0.1", 5000)])
    benchmark_throughput(c)
    stop_server(p, True)
    print("Running durability test with random kills (may take a while)...")
    durability_test_with_random_kill()