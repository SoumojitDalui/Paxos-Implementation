import asyncio, csv, sys
from typing import Any, Dict, List, Optional
from common import (
    ALL_NODE_IDS,
    CLIENT_LISTEN_PORT,
    ScenarioSet,
    ScenarioStep,
    node_port,
)
from client import Client
from node import Node
from transport import send_message


class ClusterController:
    def __init__(self, clients: List[Client]):
        self.clients = clients
        self.active_nodes: List[int] = list(ALL_NODE_IDS)
        self.default_leader = ALL_NODE_IDS[0]
        self.node_tasks: List[asyncio.Task] = []
        self.nodes: Dict[int, Node] = {}
        self.backlog: List[Dict[str, Any]] = []

    async def start_nodes_concurrently(self):
        if self.node_tasks:
            return
        for nid in ALL_NODE_IDS:
            n = Node(nid, leader_init=1)
            self.nodes[nid] = n
            self.node_tasks.append(asyncio.create_task(n.start()))
        # jitter for nodes to start listening
        await asyncio.sleep(0.5)

    async def prepare_set(self, live_nodes: List[int], preserve_state: bool = True):
        if not preserve_state:
            await self.send_control("reset_state")
            await asyncio.sleep(0.4)
        to_deactivate = [nid for nid in ALL_NODE_IDS if nid not in live_nodes]
        if to_deactivate:
            await self.send_control("set_active", targets=to_deactivate, active=False)
        await asyncio.sleep(0.25)
        to_activate = list(live_nodes)
        if to_activate:
            await self.send_control("set_active", targets=to_activate, active=True)
        self.active_nodes = to_activate
        await asyncio.sleep(0.6)
        self.default_leader = (self.active_nodes[0] if self.active_nodes else ALL_NODE_IDS[0])

    async def stop_nodes(self):
        if not self.node_tasks:
            return
        for t in list(self.node_tasks):
            try:
                t.cancel()
            except Exception:
                pass
        try:
            await asyncio.gather(*self.node_tasks, return_exceptions=True)
        finally:
            self.node_tasks.clear()
            self.nodes.clear()

    async def send_control(self, action: str, targets: Optional[List[int]] = None, **extra: Any):
        payload = {"action": action}
        payload.update(extra)
        node_ids = targets if targets is not None else ALL_NODE_IDS
        tasks = []
        for nid in node_ids:
            msg = {"type": "CONTROL", "from": 0, "to": nid, "payload": payload}
            tasks.append(send_message("127.0.0.1", node_port(nid), msg))
        await asyncio.gather(*tasks, return_exceptions=True)

    async def wait_for_clients(self, settle_timeout: Optional[float] = 4.0):
        start = asyncio.get_event_loop().time()
        loop = asyncio.get_event_loop()
        while True:
            any_pending = False
            for c in self.clients:
                for t in list(c.pending_wait.values()):
                    if not getattr(t, "done", lambda: True)():
                        any_pending = True
                        break
                if any_pending:
                    break
            if not any_pending:
                return
            if settle_timeout is not None and (loop.time() - start) > settle_timeout:
                print("[WARN] wait_for_clients timeout; continuing")
                return
            await asyncio.sleep(0.05)

    def has_quorum(self) -> bool:
        return len(self.active_nodes) >= 3

    def _client_for_source(self, s: str) -> Client:
        idx = 0
        if s:
            idx = max(0, min(9, ord(s[0].upper()) - ord('A')))
        return self.clients[idx]

    async def flush_backlog(self):
        if not self.backlog:
            return
        print(f"[I] Flushing {len(self.backlog)} queued txn(s) before current set")
        for txn in list(self.backlog):
            s = (txn.get("s") or "").strip()
            r = (txn.get("r") or "").strip()
            amt = int(txn.get("amt", 0))
            if not s:
                continue
            client = self._client_for_source(s)
            print(f"[BACKLOG] {s} -> {r} amt={amt} via {client.client_id}")
            await client.send_txn(s, r, amt)
            await asyncio.sleep(0.05)
        self.backlog.clear()
        await self.wait_for_clients(settle_timeout=None)


def parse_csv_sets(path: str) -> List[ScenarioSet]:
    def _parse_txn(raw: str):
        raw = raw.strip()
        if not raw:
            return None
        if raw.upper() == "LF":
            return {"event": "LF"}
        if not (raw.startswith("(") and raw.endswith(")")):
            raise ValueError(f"Unexpected transaction format: {raw}")
        body = raw[1:-1]
        parts = [p.strip() for p in body.split(",")]
        if len(parts) != 3:
            raise ValueError(f"Transaction should have 3 parts: {raw}")
        s, r, amt_str = parts
        amt = int(amt_str)
        return {"s": s, "r": r, "amt": amt}

    def _parse_live(raw: str) -> List[int]:
        raw = raw.strip()
        if not raw:
            return []
        if raw.startswith("[") and raw.endswith("]"):
            raw = raw[1:-1]
        nodes = []
        for token in raw.split(","):
            token = token.strip().lstrip("n").strip()
            if not token:
                continue
            nodes.append(int(token))
        return nodes

    sets: Dict[int, Dict[str, Any]] = {}
    with open(path, "r", encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh)
        current_set: Optional[int] = None
        for row in reader:
            raw_set = (row.get("Set Number") or "").strip()
            if raw_set:
                current_set = int(raw_set)
            if current_set is None:
                continue
            entry = sets.setdefault(current_set, {"live": [], "steps": []})
            live_raw = (row.get("Live Nodes") or "").strip()
            if live_raw:
                entry["live"] = _parse_live(live_raw)
            txn_raw = (row.get("Transactions") or "").strip()
            if not txn_raw:
                continue
            parsed = _parse_txn(txn_raw)
            if parsed is None:
                continue
            if "event" in parsed:
                entry["steps"].append(ScenarioStep(kind="event", name=parsed["event"]))
            else:
                entry["steps"].append(ScenarioStep(kind="txn", txn=parsed))

    scenario_sets: List[ScenarioSet] = []
    for key in sorted(sets):
        info = sets[key]
        scenario_sets.append(ScenarioSet(set_id=key, live_nodes=info["live"], steps=info["steps"]))
    return scenario_sets


async def run_csv_interactive(path: str, start_nodes: bool = True, preserve_state: bool = True):
    # Create 10 clients restricted to A..J
    clients = [
        Client(leader=1, listen_port=CLIENT_LISTEN_PORT + i, client_id=f"cli{chr(ord('A')+i)}", allowed_prefix=chr(ord('A')+i))
        for i in range(10)
    ]
    for c in clients:
        await c.start()
    controller = ClusterController(clients)
    if start_nodes:
        if not controller.node_tasks:
            for nid in ALL_NODE_IDS:
                n = Node(nid, leader_init=1)
                controller.nodes[nid] = n
                controller.node_tasks.append(asyncio.create_task(n.start()))
            await asyncio.sleep(0.5)
    scenario_sets = parse_csv_sets(path)
    print(f"[I] Loaded {len(scenario_sets)} set(s) from {path}")

    current_leader = controller.default_leader
    for scenario in scenario_sets:
        print(f"\n[CASE] Ready: set {scenario.set_id}")
        print("Press Enter to run this set (or q then Enter to quit): ", end="", flush=True)
        loop = asyncio.get_running_loop()
        line = await loop.run_in_executor(None, sys.stdin.readline)
        if not line or line.strip().lower() == "q":
            print("[I] Stopping before running this set")
            break

        if scenario.live_nodes:
            live_list = ", ".join(f"n{nid}" for nid in scenario.live_nodes)
            print(f"[I] Set {scenario.set_id} live nodes: [{live_list}]")
        else:
            print(f"[I] Set {scenario.set_id} live nodes not specified; using current cluster state")

        if not preserve_state:
            await controller.send_control("reset_state")
            await asyncio.sleep(0.4)
        if scenario.live_nodes:
            to_deactivate = [nid for nid in ALL_NODE_IDS if nid not in scenario.live_nodes]
            if to_deactivate:
                await controller.send_control("set_active", targets=to_deactivate, active=False)
            await asyncio.sleep(0.25)
            to_activate = list(scenario.live_nodes)
            if to_activate:
                await controller.send_control("set_active", targets=to_activate, active=True)
            controller.active_nodes = to_activate
        current_leader = (controller.active_nodes[0] if controller.active_nodes else ALL_NODE_IDS[0])
        await asyncio.sleep(1.1)
        controller.default_leader = current_leader
        await controller.send_control("clear_view_history")

        if not controller.has_quorum():
            print("[WARN] No quorum (need >=3). Queuing this set's transactions for later.")
            for step in scenario.steps:
                if not step.is_event and step.txn:
                    controller.backlog.append(step.txn)
            continue

        for c in controller.clients:
            try:
                await c.on_leader_change(current_leader)
            except Exception:
                pass
        await controller.flush_backlog()

        txn_index = 0
        last_for_client: Dict[str, asyncio.Task] = {}
        slow_after_lf = False
        buffer_rest = False
        for step in scenario.steps:
            if buffer_rest and (not step.is_event) and step.txn:
                txn = step.txn
                s = (txn.get("s") or "").strip()
                r = (txn.get("r") or "").strip()
                amt = int(txn.get("amt", 0))
                print(f"[BACKLOG-LF] No quorum; buffering txn: {s} -> {r} amt={amt}")
                controller.backlog.append(txn)
                continue
            if step.is_event:
                if step.name.upper() == "LF":
                    # Previous transactions are completed before LF
                    if last_for_client:
                        await asyncio.gather(*last_for_client.values(), return_exceptions=True)
                        last_for_client.clear()
                    await controller.wait_for_clients()

                    print("[EVENT] Simulating leader failure (leader stays down until next set)")
                    await controller.send_control("set_active", targets=[current_leader], active=False)
                    try:
                        controller.active_nodes = [n for n in controller.active_nodes if n != current_leader]
                    except Exception:
                        pass
                    # Jitter to lessen the logs
                    wait_after = 3.0
                    await asyncio.sleep(wait_after)
                    candidates = controller.active_nodes or ALL_NODE_IDS
                    if current_leader in candidates and len(candidates) > 1:
                        idx = candidates.index(current_leader)
                        next_leader = candidates[(idx + 1) % len(candidates)]
                    else:
                        next_leader = candidates[0]
                    await asyncio.sleep(1.6)
                    current_leader = next_leader
                    # Proactively trigger an election across active nodes to ensure a leader is chosen
                    if controller.active_nodes:
                        await controller.send_control("force_election", targets=controller.active_nodes)
                        # small settle time for Prepare/NEW_VIEW to complete
                        await asyncio.sleep(0.6)
                    slow_after_lf = True
                    for c in controller.clients:
                        try:
                            await c.on_leader_change(current_leader)
                        except Exception:
                            pass
                    # If no quorum, buffer the rest to next set (print only once)
                    if len(controller.active_nodes) < 3:
                        print("[WARN] No quorum after LF (need >=3). Buffering remaining transactions for next set.")
                        buffer_rest = True
                    continue
                # Ignore unknown events
                continue

            txn = step.txn or {}
            txn_index += 1
            s = (txn.get("s") or "").strip()
            r = (txn.get("r") or "").strip()
            amt = int(txn.get("amt", 0))
            if not s:
                continue
            if slow_after_lf:
                await asyncio.sleep(0.25)
            client = controller._client_for_source(s)
            print(f"[CASE] Set {scenario.set_id} txn {txn_index}: {s} -> {r} amt={amt} via {client.client_id}")
            key = client.client_id
            prev = last_for_client.get(key)
            if prev is None:
                last_for_client[key] = asyncio.create_task(client.send_txn_and_wait(s, r, amt))  # type: ignore[attr-defined]
            else:
                async def _chain(prev_task: asyncio.Task, _s: str, _r: str, _amt: int, _c: Client):
                    try:
                        await prev_task
                    except Exception:
                        pass
                    await _c.send_txn_and_wait(_s, _r, _amt)  # type: ignore[attr-defined]
                last_for_client[key] = asyncio.create_task(_chain(prev, s, r, amt, client))

        await controller.wait_for_clients()
        if last_for_client:
            await asyncio.gather(*last_for_client.values(), return_exceptions=True)
        await asyncio.sleep(0.3)
