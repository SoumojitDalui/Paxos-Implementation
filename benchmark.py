import asyncio, random, time
from typing import Any, Dict, List
from client import Client
from common import set_log_quiet
from common import CLIENT_LISTEN_PORT, INITIAL_ACCOUNTS
from controller import ClusterController


def _zipf_weights(n: int, s: float) -> List[float]:
    w = [1.0 / ((i + 1) ** s) for i in range(n)]
    total = sum(w)
    return [x / total for x in w]


class BenchClient(Client):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.ts_start: Dict[int, float] = {}
        self.latencies_ms: List[float] = []
        self.completed: int = 0

    async def send_txn(self, s: str, r: str, amt: int):
        ts = self.ts
        self.ts_start[ts] = time.time()
        await super().send_txn(s, r, amt)

    async def _on_reply(self, payload: Dict[str, Any]):
        ts = int(payload.get("ts", 0))
        st = self.ts_start.pop(ts, None)
        if st is not None:
            self.latencies_ms.append((time.time() - st) * 1000.0)
            self.completed += 1


async def run_smallbank_benchmark(ops: int = 20, concurrency: int = 10, zipf_s: float = 0.9):
    # Quiet logging during benchmark
    set_log_quiet(True)
    n_clients = min(10, max(1, concurrency))
    clients: List[BenchClient] = [
        BenchClient(leader=1, listen_port=CLIENT_LISTEN_PORT + i, client_id=f"bench{i}")
        for i in range(n_clients)
    ]
    for c in clients:
        await c.start()
    controller = ClusterController(clients)# type: ignore[attr-defined]
    await controller.start_nodes_concurrently() # type: ignore[attr-defined]
    await controller.prepare_set([1, 2, 3, 4, 5], preserve_state=True)  # type: ignore[attr-defined]

    accounts = list(INITIAL_ACCOUNTS)
    weights = _zipf_weights(len(accounts), zipf_s)
    pop_accounts = accounts.copy()

    def pick_account() -> str:
        r = random.random()
        cum = 0.0
        for acc, w in zip(pop_accounts, weights):
            cum += w
            if r <= cum:
                return acc
        return pop_accounts[-1]

    start = time.time()
    op_queue: asyncio.Queue[int] = asyncio.Queue()
    for i in range(ops):
        op_queue.put_nowait(i)

    async def worker():
        while True:
            try:
                _ = op_queue.get_nowait()
            except asyncio.QueueEmpty:
                return
            # Distinct accounts are picked
            s = pick_account()
            r = pick_account()
            while r == s:
                r = pick_account()
            amt = random.randint(1, 3)
            client = random.choice(clients)
            await client.send_txn(s, r, amt)
            await asyncio.sleep(0)

    workers = [asyncio.create_task(worker()) for _ in range(concurrency)]
    await asyncio.gather(*workers)
    await controller.wait_for_clients(settle_timeout=10.0)
    elapsed = max(1e-6, time.time() - start)

    all_lat = []
    done = 0
    for c in clients:
        all_lat.extend(c.latencies_ms)
        done += c.completed
    all_lat.sort()
    def pct(p: float) -> float:
        if not all_lat:
            return 0.0
        k = min(len(all_lat) - 1, int(len(all_lat) * p))
        return all_lat[k]

    print("\n[BENCH] SmallBank-like transfer workload")
    print(f" ops={done}/{ops} concurrency={concurrency} zipf_s={zipf_s}")
    print(f" throughput={done/elapsed:.1f} ops/s elapsed={elapsed:.2f}s")
    if all_lat:
        print(f" p50={pct(0.50):.1f}ms p95={pct(0.95):.1f}ms p99={pct(0.99):.1f}ms max={all_lat[-1]:.1f}ms")

    for c in clients:
        try:
            await c.server.stop()
        except Exception:
            pass
    try:
        await controller.stop_nodes()  # type: ignore[attr-defined]
    except Exception:
        pass
