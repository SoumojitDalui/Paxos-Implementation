import asyncio, random
from typing import Any, Dict, Set
from common import (
    ALL_NODE_IDS,
    CLIENT_LISTEN_PORT,
    REDIRECT,
    REPLY,
    REQUEST,
    CLIENT_LEADER,
    node_port,
    LOG_QUIET,
    parse_ballot,
)
from transport import Server, send_message


class Client:
    def __init__(self, leader: int = 1, listen_port: int = CLIENT_LISTEN_PORT, client_id: str = "cli", allowed_prefix: str = ""):
        self.assumed_leader = leader
        self.listen_port = listen_port
        self.client_id = client_id
        self.allowed_prefix = allowed_prefix.upper() if allowed_prefix else ""
        self.server = Server(listen_port)
        self.ts = 1
        self.all_nodes = list(ALL_NODE_IDS)

        self.reply_events: Dict[int, asyncio.Event] = {}
        self.pending_wait: Dict[int, asyncio.Task] = {}
        self.retry_base_s = 1.2
        self.max_retries = 7
        self.outstanding_payloads: Dict[int, Dict[str, Any]] = {}
        self.seen_reply_ts: Set[int] = set()
        self.redirect_jitter_range = (0.10, 0.25)
        self.timeout_jitter_range = (0.08, 0.18)

    async def start(self):
        await self.server.start()
        if not LOG_QUIET:
            print(f"[I] Client {self.client_id} ready; listening on {self.listen_port}")
        asyncio.create_task(self._reply_loop())

    def reset_state(self, new_leader=None):
        for task in self.pending_wait.values():
            task.cancel()
        self.pending_wait.clear()
        self.reply_events.clear()
        self.ts = 1
        if new_leader is not None:
            self.assumed_leader = new_leader
        self.outstanding_payloads.clear()
        self.seen_reply_ts.clear()
        

    async def on_leader_change(self, leader: int):
        if self.assumed_leader == leader:
            return
        self.assumed_leader = leader
        await self._resend_outstanding()

    async def _reply_loop(self):
        while True:
            msg = await self.server.queue.get()
            t = msg.get("type")
            if t == REPLY:
                p = msg["payload"]
                ts = int(p.get("ts", 0))
                try:
                    bnode = parse_ballot(str(p.get("ballot", "0:0")))[1]
                    if bnode and bnode != self.assumed_leader:
                        await self.on_leader_change(bnode)
                except Exception:
                    pass
                if ts not in self.seen_reply_ts:
                    if not LOG_QUIET:
                        print(f"[REPLY] port={self.listen_port} ballot={p['ballot']} ts={p['ts']} client={p['client']} result={p['result']}")
                    self.seen_reply_ts.add(ts)
                try:
                    await self._on_reply(p)  # type: ignore[attr-defined]
                except AttributeError:
                    pass
                ev = self.reply_events.get(ts)
                if ev:
                    ev.set()
                self.outstanding_payloads.pop(ts, None)
                self.reply_events.pop(ts, None)
                self.pending_wait.pop(ts, None)
            elif t == REDIRECT:
                p = msg.get("payload", {})
                leader = int(p.get("leader", 0))
                ts = int(p.get("ts", 0))
                if leader:
                    if leader != self.assumed_leader:
                        await self.on_leader_change(leader)
                        asyncio.create_task(self._fanout_leader(leader))
                        payload = self.outstanding_payloads.get(ts)
                        if payload:
                            await asyncio.sleep(random.uniform(*self.redirect_jitter_range))
                            await self._send_to_leader(payload)
            elif t == CLIENT_LEADER:
                p = msg.get("payload", {})
                leader = int(p.get("leader", 0))
                if leader and leader != self.assumed_leader:
                    await self.on_leader_change(leader)

    async def send_txn(self, s: str, r: str, amt: int):
        if self.allowed_prefix and (not s or s[0].upper() != self.allowed_prefix):
            if not LOG_QUIET:
                print(f"[SKIP] {self.client_id} restricted to {self.allowed_prefix}*, got {s}")
            return
        payload = {"ts": self.ts, "s": s, "r": r, "amt": amt, "client": self.client_id, "client_port": self.listen_port}
        ts = self.ts
        self.ts += 1
        self.outstanding_payloads[ts] = payload
        await self._send_to_leader(payload)

        ev = asyncio.Event()
        self.reply_events[ts] = ev
        self.pending_wait[ts] = asyncio.create_task(self._wait_and_retry(payload, ev))

    async def _send_to_leader(self, payload: Dict[str, Any]):
        leader = self.assumed_leader
        msg = {"type": REQUEST, "from": 0, "to": leader, "payload": payload}
        try:
            await send_message("127.0.0.1", node_port(leader), msg)
            if not LOG_QUIET:
                print(f"[I] {self.client_id} -> n{leader} (leader=n{leader}): REQUEST {payload['s']}->{payload['r']}:{payload['amt']} ts={payload['ts']}")
        except OSError:
            if not LOG_QUIET:
                print(f"[WARN] {self.client_id}: leader n{leader} unreachable; rebroadcasting to cluster (no client-led election)")
            await self._rebroadcast_request(payload)

    async def _fanout_leader(self, leader: int):
        tasks = []
        for i in range(10):
            port = CLIENT_LISTEN_PORT + i
            payload = {"leader": leader}
            msg = {"type": CLIENT_LEADER, "from": self.listen_port, "payload": payload}
            tasks.append(send_message("127.0.0.1", port, msg))
        await asyncio.gather(*tasks, return_exceptions=True)

    async def _resend_outstanding(self):
        for ts in sorted(self.outstanding_payloads.keys()):
            await self._send_to_leader(self.outstanding_payloads[ts])

    async def _wait_and_retry(self, payload: Dict[str, Any], ev: asyncio.Event):
        timeout = self.retry_base_s
        tries = 0
        while True:
            try:
                await asyncio.wait_for(ev.wait(), timeout=timeout)
                return
            except asyncio.TimeoutError:
                tries += 1
                if tries > self.max_retries:
                    print("[WARN] client retry timed out; request may still be processing (idempotent on future retry).")
                    break
                # Rebroadcast to all nodes so any follower can redirect/forward
                await asyncio.sleep(random.uniform(*self.timeout_jitter_range))
                await self._rebroadcast_request(payload)
                timeout *= 1.5
        ts = int(payload["ts"])
        self.reply_events.pop(ts, None)
        self.pending_wait.pop(ts, None)

    async def send_txn_and_wait(self, s: str, r: str, amt: int):
        ts = self.ts
        await self.send_txn(s, r, amt)
        task = self.pending_wait.get(ts)
        if task is not None:
            try:
                await task
            except asyncio.CancelledError:
                pass

    async def _rebroadcast_request(self, payload: Dict[str, Any]):
        # silent rebroadcast to all nodes; clients will converge via redirects/replies
        tasks = []
        for nid in self.all_nodes:
            msg = {"type": REQUEST, "from": 0, "to": nid, "payload": payload}
            tasks.append(send_message("127.0.0.1", node_port(nid), msg))
        await asyncio.gather(*tasks, return_exceptions=True)
