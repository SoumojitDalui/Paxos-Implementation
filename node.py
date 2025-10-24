import asyncio, json, time, random, hashlib
from copy import deepcopy
from typing import Any, Dict, List, Optional, Set, Tuple
from common import (
    ACCEPT,
    ACCEPTED,
    ACK,
    ALL_NODE_IDS,
    CLIENT_LISTEN_PORT,
    CHECKPOINT,
    COMMIT,
    CONTROL,
    HEARTBEAT,
    NEW_VIEW,
    NOOP_REQ,
    PREPARE,
    REDIRECT,
    REPLY,
    REQUEST,
    EntryStatus,
    INITIAL_ACCOUNTS,
    INITIAL_BALANCE,
    CHECKPOINT_PERIOD,
    ballot_ge,
    ballot_gt,
    ballot_str,
    max_ballot,
    node_port,
    short_req,
)
from transport import Server, send_message
from common import LOG_QUIET


class Node:
    def __init__(self, node_id: int, all_nodes: List[int] = ALL_NODE_IDS, f: int = 2, leader_init: int = 1):
        self.id = node_id
        self.all_nodes = list(all_nodes)
        self.f = f
        self.server = Server(node_port(node_id))

        self.active = True
        self.name = f"n{self.id}"

        self.leader_id = leader_init
        self.term = 1
        self.ballot = ballot_str(self.term, self.id)
        self.max_ballot_seen = ballot_str(0, 0)
        self.last_leader_msg = time.time()
        self.heartbeat_ms = 150
        self.election_timeout_ms = 2200
        self.tp_ms = 500
        self.last_prepare_seen = 0.0

        self.log: List[Dict[str, Any]] = []
        # Holds entries that were compacted by checkpoints
        self.compacted_log: List[Dict[str, Any]] = []
        # History of checkpoints
        self.checkpoint_history: List[Dict[str, Any]] = []
        self.next_seq = 1
        self.last_executed_seq = 0
        self._db = {acc: INITIAL_BALANCE for acc in INITIAL_ACCOUNTS}

        self.last_reply: Dict[str, Dict[str, Any]] = {}
        self.leader_inflight: Dict[Tuple[str, int], int] = {}

        self.accepted_votes: Dict[str, Dict[int, Set[int]]] = {}
        self.committed_broadcasted: Dict[str, Set[int]] = {}
        self.accepted_per_seq: Dict[int, Dict[str, Any]] = {}
        self._acks_state: Dict[str, Dict[int, Dict[str, Any]]] = {}
        self._acks_senders: Dict[str, Set[int]] = {}
        self._acks_max_seq: Dict[str, int] = {}
        self.new_view_history: List[str] = []
        self.last_checkpoint_seq = 0

        self.accept_retry_base_ms = 400
        self.accept_retry_max_ms = 2000
        self.accept_retry_max_attempts = 5
        self.pending_accept_tasks: Dict[str, Dict[int, asyncio.Task]] = {}

        self.prepare_retry_base_ms = 500
        self.prepare_retry_max_ms = 3000
        self.prepare_retry_max_attempts = 6
        self._prepare_task: Optional[asyncio.Task] = None
        self._buffered_prepare: Optional[Dict[str, Any]] = None

    async def start(self):
        await self.server.start()
        if self.leader_id:
            print(f"[I] Node {self.id} up. leader=n{self.leader_id}")
        else:
            print(f"[I] Node {self.id} up. awaiting leader heartbeat")
        await asyncio.gather(self.loop(), self.heartbeat_loop())

    async def loop(self):
        while True:
            try:
                msg = await asyncio.wait_for(self.server.queue.get(), timeout=0.05)
                if not self.active:
                    if msg.get("type") == CONTROL:
                        await self.handle_control(msg)
                    continue
                await self.on_message(msg)
            except asyncio.TimeoutError:
                if self.id != self.leader_id and self.active:
                    if (time.time() - self.last_leader_msg) * 1000.0 > self.election_timeout_ms:
                        if self._buffered_prepare is not None:
                            try:
                                await self._ack_prepare(self._buffered_prepare)
                            except Exception:
                                # Ignore errors when proposer is unreachable
                                pass
                            finally:
                                self._buffered_prepare = None
                        else:
                            await self.maybe_start_election()
                        await asyncio.sleep(random.uniform(0.05, 0.12))

    async def heartbeat_loop(self):
        while True:
            await asyncio.sleep(self.heartbeat_ms / 1000.0)
            if not self.active:
                continue
            if self.id == self.leader_id:
                hb = {"type": HEARTBEAT, "from": self.id, "payload": {"kind": "hb", "ballot": self.ballot}}
                await self.broadcast(hb)

    async def on_message(self, m: Dict[str, Any]):
        sender = m.get("from")
        if sender == self.leader_id:
            self.last_leader_msg = time.time()

        t = m.get("type")
        if t == REQUEST:
            await self.handle_request(m)
        elif t == CHECKPOINT:
            await self.handle_checkpoint(m)
        elif t == PREPARE:
            await self.handle_prepare(m)
        elif t == ACK:
            await self.handle_ack(m)
        elif t == ACCEPT:
            await self.handle_accept(m)
        elif t == ACCEPTED:
            await self.handle_accepted(m)
        elif t == COMMIT:
            await self.handle_commit(m)
        elif t == NEW_VIEW:
            await self.handle_new_view(m)
        elif t == CONTROL:
            await self.handle_control(m)
        elif t == HEARTBEAT:
            await self.handle_HEARTBEAT(m)

    async def handle_request(self, m: Dict[str, Any]):
        req = m["payload"]
        ts = int(req.get("ts", 0))
        client = str(req.get("client", "anon"))
        client_port = int(req.get("client_port", CLIENT_LISTEN_PORT))

        last = self.last_reply.get(client)
        if last:
            last_ts = int(last["ts"])
            if ts == last_ts:
                port = int(last.get("port", CLIENT_LISTEN_PORT))
                await self.send_reply(client, last_ts, last["result"], ballot=last["ballot"], port=port)
                return
            if ts < last_ts:
                return

        if self.id != self.leader_id:
            if self.leader_id and self.leader_id in self.all_nodes:
                try:
                    redir = {"type": REDIRECT, "from": self.id, "payload": {"leader": self.leader_id, "ts": ts, "client": client}}
                    await send_message("127.0.0.1", client_port, redir)
                except Exception:
                    pass
                fwd = {"type": REQUEST, "from": self.id, "to": self.leader_id, "payload": req}
                try:
                    await send_message("127.0.0.1", node_port(self.leader_id), fwd)
                except Exception:
                    await self.maybe_start_election()
            else:
                await self.maybe_start_election()
                tasks = []
                for nid in self.all_nodes:
                    fwd = {"type": REQUEST, "from": self.id, "to": nid, "payload": req}
                    tasks.append(send_message("127.0.0.1", node_port(nid), fwd))
                await asyncio.gather(*tasks, return_exceptions=True)
            return

        key = (client, ts)
        if key in self.leader_inflight:
            return

        seq = await self.leader_broadcast_accept(deepcopy(req))
        if client:
            self.leader_inflight[key] = seq

    async def handle_prepare(self, m: Dict[str, Any]):
        self.last_prepare_seen = time.time()
        b = m["payload"]["ballot"]
        proposer = m["from"]
        if not ballot_gt(b, self.max_ballot_seen):
            return
        if (time.time() - self.last_leader_msg) * 1000.0 <= self.election_timeout_ms:
            if self._buffered_prepare is None or ballot_gt(b, self._buffered_prepare["payload"]["ballot"]):
                self._buffered_prepare = m
            return
        await self._ack_prepare(m)

    async def handle_ack(self, m: Dict[str, Any]):
        p = m["payload"]
        ack_ballot = p["ballot"]
        if ack_ballot != self.ballot:
            return
        union = self._acks_state.setdefault(ack_ballot, {})
        max_seq = self._acks_max_seq.get(ack_ballot, 0)
        cp_n = 0
        try:
            cp_n = int(p.get("checkpoint_n", 0))
        except Exception:
            cp_n = 0
        prev_cp = self._acks_state.setdefault("_cp_n", {}).get(ack_ballot, 0)
        if cp_n > prev_cp:  # type: ignore[attr-defined]
            self._acks_state.setdefault("_cp_n", {})[ack_ballot] = cp_n     # type: ignore[index]
        for entry in p["accept_log"]:
            seq = int(entry["seq"])
            max_seq = max(max_seq, seq)
            accn = entry["accept_num"]
            cur = union.get(seq)
            if cur is None or ballot_gt(accn, cur["accept_num"]):
                req_payload = entry.get("req")
                union[seq] = {"accept_num": accn, "req": deepcopy(req_payload) if req_payload is not None else deepcopy(NOOP_REQ)}
        self._acks_max_seq[ack_ballot] = max_seq
        senders = self._acks_senders.setdefault(ack_ballot, set())
        senders.add(m["from"])
        quorum_count = len(senders) + (0 if self.id in senders else 1)
        if quorum_count >= self.f + 1:
            accept_entries = []
            start_seq = max(self._acks_state.get("_cp_n", {}).get(ack_ballot, 0) + 1, 1)    # type: ignore[attr-defined]
            for seq in range(start_seq, max_seq + 1):
                stored = union.get(seq)
                accept_entries.append({"seq": seq, "req": stored["req"] if stored else NOOP_REQ})
            cp_use = self._acks_state.get("_cp_n", {}).get(ack_ballot, 0)
            payload = {"ballot": self.ballot, "entries": accept_entries, "checkpoint_n": cp_use}
            if cp_use and self.last_checkpoint_seq >= cp_use:   # type: ignore[attr-defined]
                payload["checkpoint_db"] = deepcopy(self._db)
                try:
                    payload["checkpoint_digest"] = hashlib.sha256(json.dumps(self._db, sort_keys=True).encode()).hexdigest()
                except Exception:
                    payload["checkpoint_digest"] = ""
            nv = {"type": NEW_VIEW, "from": self.id, "payload": payload}
            await self.broadcast(nv)
            self.new_view_history.append(json.dumps(nv["payload"]))
            self._acks_state.pop(ack_ballot, None)
            self._acks_senders.pop(ack_ballot, None)
            self._acks_max_seq.pop(ack_ballot, None)
            self.leader_id = self.id
            self.last_leader_msg = time.time()
            self._cancel_prepare_task()
            if not LOG_QUIET:
                print(f"[LEADER] n{self.id} elected (ballot={self.ballot})")
            self.next_seq = max(self.next_seq, max_seq + 1)

    async def handle_accept(self, m: Dict[str, Any]):
        ap = m["payload"]
        b = ap["ballot"]
        seq = int(ap["seq"])
        req = ap["req"]
        if ballot_ge(b, self.max_ballot_seen):
            self.max_ballot_seen = b
            self._upsert_log(seq, b, req, EntryStatus.A)
            self.accepted_per_seq[seq] = {"accept_num": b, "req": deepcopy(req)}
            ack = {"type": ACCEPTED, "from": self.id, "to": m["from"], "payload": {"ballot": b, "seq": seq}}
            await send_message("127.0.0.1", node_port(m["from"]), ack)

    async def handle_accepted(self, m: Dict[str, Any]):
        ap = m["payload"]
        b = ap["ballot"]
        seq = int(ap["seq"])
        if b != self.ballot:
            return
        voters_map = self.accepted_votes.setdefault(b, {})
        voters = voters_map.setdefault(seq, set())
        if m["from"] != self.id:
            voters.add(m["from"])
        quorum = len(voters) + 1
        if quorum >= self.f + 1:
            committed_set = self.committed_broadcasted.setdefault(b, set())
            if seq not in committed_set:
                committed_set.add(seq)
                self._cancel_accept_task(b, seq)
                req = self._get_req_by_seq(seq) or NOOP_REQ
                self._upgrade_status(seq, EntryStatus.C)
                cm = {"type": COMMIT, "from": self.id, "payload": {"ballot": b, "seq": seq, "req": req}}
                await self.broadcast(cm)
                await self.execute_ready_prefix()

    async def handle_commit(self, m: Dict[str, Any]):
        seq = int(m["payload"]["seq"])
        req = m["payload"].get("req")
        b = m["payload"].get("ballot", self.ballot)
        if req is not None:
            self._ensure_req(seq, b, req)
        self._cancel_accept_task(b, seq)
        self._upgrade_status(seq, EntryStatus.C)
        await self.execute_ready_prefix()

    async def handle_new_view(self, m: Dict[str, Any]):
        p = m["payload"]
        b = p["ballot"]
        cp_n = int(p.get("checkpoint_n", 0))
        cp_db = p.get("checkpoint_db")
        cp_dig = p.get("checkpoint_digest")
        entries = p.get("entries", [])
        self.max_ballot_seen = max_ballot(b, self.max_ballot_seen)
        if cp_n > self.last_checkpoint_seq and isinstance(cp_db, dict):
            if isinstance(cp_dig, str) and cp_dig:
                try:
                    calc = hashlib.sha256(json.dumps(cp_db, sort_keys=True).encode()).hexdigest()
                except Exception:
                    calc = ""
                if calc and calc != cp_dig:
                    print(f"[WARN] {self.name} NEW_VIEW checkpoint digest mismatch for n={cp_n}: got={cp_dig} calc={calc}")
                else:
                    await self._apply_checkpoint(cp_n, cp_db)
            else:
                await self._apply_checkpoint(cp_n, cp_db)
        for e in entries:
            seq = int(e["seq"])
            req = deepcopy(e["req"])
            self._upsert_log(seq, b, req, EntryStatus.A)
            self.accepted_per_seq[seq] = {"accept_num": b, "req": deepcopy(req)}
            ack = {"type": ACCEPTED, "from": self.id, "to": m["from"], "payload": {"ballot": b, "seq": seq}}
            await send_message("127.0.0.1", node_port(m["from"]), ack)
        self.leader_id = m["from"]
        self.last_leader_msg = time.time()
        print(f"[I] n{self.id} now believes leader is n{self.leader_id} (ballot={b})")
        for b_tasks in self.pending_accept_tasks.values():
            for t in list(b_tasks.values()):
                t.cancel()
        self.pending_accept_tasks.clear()
        self._cancel_prepare_task()
        self.new_view_history.append(json.dumps(m["payload"]))
        if entries:
            mx = max(int(e["seq"]) for e in entries)
            self.next_seq = max(self.next_seq, mx + 1)

    async def handle_control(self, m: Dict[str, Any]):
        payload = m.get("payload", {})
        action = payload.get("action", "")
        if action == "reset_state":
            self.reset_state()
            print(f"[CTRL] {self.name} state reset by sender {m.get('from', '?')}")
        elif action == "set_active":
            desired = bool(payload.get("active", True))
            if self.active != desired:
                self.set_active(desired)
        elif action == "fail_leader":
            duration = float(payload.get("duration_ms", 700)) / 1000.0
            if self.id == self.leader_id and self.active:
                asyncio.create_task(self._simulate_leader_failure(duration))
        elif action == "clear_view_history":
            self.new_view_history.clear()
        elif action == "force_election":
            # Proactively trigger a Prepare round to ensure leadership emerges.
            # Bypass timing guard by resetting last_prepare_seen.
            self.last_prepare_seen = 0.0
            await self.maybe_start_election()

    async def handle_HEARTBEAT(self, m: Dict[str, Any]):
        sender = int(m.get("from", 0) or 0)
        if not sender or sender == self.id:
            return
        p = m.get("payload", {})
        b = str(p.get("ballot", "0:0"))
        self.last_leader_msg = time.time()
        if self.leader_id != sender:
            self.leader_id = sender
            if not LOG_QUIET:
                print(f"[I] {self.name} now follows leader n{self.leader_id}")
        # Suppress stale self-elections
        if b:
            self.max_ballot_seen = max_ballot(b, self.max_ballot_seen)
            # If preparing with a lower ballot
            if not ballot_ge(self.ballot, b):
                self._cancel_prepare_task()

    def reset_state(self):
        for ballot_tasks in self.pending_accept_tasks.values():
            for task in ballot_tasks.values():
                task.cancel()
        self.pending_accept_tasks.clear()
        self._cancel_prepare_task()
        self.log.clear()
        self.next_seq = 1
        self.last_executed_seq = 0
        self.last_checkpoint_seq = 0
        self.leader_inflight.clear()
        self.accepted_votes.clear()
        self.committed_broadcasted.clear()
        self.accepted_per_seq.clear()
        self._acks_state.clear()
        self._acks_senders.clear()
        self._acks_max_seq.clear()
        self.last_reply.clear()
        self.new_view_history.clear()
        self.last_prepare_seen = 0.0
        self._db = {acc: INITIAL_BALANCE for acc in INITIAL_ACCOUNTS}
        self.term = 1
        self.ballot = ballot_str(self.term, self.id)
        self.max_ballot_seen = ballot_str(0, 0)
        self.leader_id = 0
        self.last_leader_msg = 0.0

    async def _simulate_leader_failure(self, duration: float):
        if not self.active:
            return
        print(f"[CTRL] {self.name} simulating leader failure for {duration:.2f}s")
        self.set_active(False)
        try:
            await asyncio.sleep(duration)
        finally:
            self.set_active(True)
            self.leader_id = 0
            self.last_leader_msg = time.time()

    async def maybe_start_election(self):
        if (time.time() - self.last_prepare_seen) * 1000.0 < self.tp_ms:
            return
        self.term += 1
        self.ballot = ballot_str(self.term, self.id)
        self._acks_state.pop(self.ballot, None)
        self._acks_senders.pop(self.ballot, None)
        self._acks_max_seq.pop(self.ballot, None)
        self._start_prepare_task()

    def _start_prepare_task(self):
        if self._prepare_task and not self._prepare_task.done():
            return
        async def _task():
            attempt = 0
            timeout = self.prepare_retry_base_ms / 1000.0
            while attempt < self.prepare_retry_max_attempts:
                attempt += 1
                prep = {"type": PREPARE, "from": self.id, "payload": {"ballot": self.ballot}}
                await self.broadcast(prep)
                await asyncio.sleep(timeout)
                if self.leader_id == self.id:
                    return
                timeout = min(timeout * 1.7, self.prepare_retry_max_ms / 1000.0)
        self._prepare_task = asyncio.create_task(_task())

    async def _ack_prepare(self, m: Dict[str, Any]):
        b = m["payload"]["ballot"]
        proposer = m["from"]
        if ballot_gt(b, self.ballot):
            self._cancel_prepare_task()
        self.max_ballot_seen = b
        acc_log = []
        for seq, rec in self.accepted_per_seq.items():
            if seq <= self.last_checkpoint_seq:
                continue
            acc_log.append({"accept_num": rec["accept_num"], "seq": seq, "req": rec["req"]})
        ack = {"type": ACK, "from": self.id, "to": proposer,
               "payload": {"ballot": b, "accept_log": acc_log, "checkpoint_n": self.last_checkpoint_seq}}
        # Only attempt send if proposer id valid
        try:
            if isinstance(proposer, int) and proposer in self.all_nodes:
                await send_message("127.0.0.1", node_port(proposer), ack)
        except Exception:
            pass

    

    def _cancel_prepare_task(self):
        if self._prepare_task:
            self._prepare_task.cancel()
            self._prepare_task = None

    async def leader_broadcast_accept(self, req: Dict[str, Any]) -> int:
        seq = self.next_seq
        self.next_seq += 1
        self._upsert_log(seq, self.ballot, req, EntryStatus.A)
        ap = {"type": ACCEPT, "from": self.id, "payload": {"ballot": self.ballot, "seq": seq, "req": req}}
        await self.broadcast(ap)
        self._schedule_accept_retry(self.ballot, seq, req)
        return seq

    def _schedule_accept_retry(self, ballot: str, seq: int, req: Dict[str, Any]):
        bmap = self.pending_accept_tasks.setdefault(ballot, {})
        if seq in bmap and not bmap[seq].done():
            return
        async def _retry():
            attempt = 0
            timeout = self.accept_retry_base_ms / 1000.0
            while attempt < self.accept_retry_max_attempts:
                await asyncio.sleep(timeout)
                attempt += 1
                ap = {"type": ACCEPT, "from": self.id, "payload": {"ballot": ballot, "seq": seq, "req": req}}
                await self.broadcast(ap)
                timeout = min(timeout * 1.7, self.accept_retry_max_ms / 1000.0)
        bmap[seq] = asyncio.create_task(_retry())

    def _cancel_accept_task(self, ballot: str, seq: int):
        t = self.pending_accept_tasks.get(ballot, {}).pop(seq, None)
        if t:
            t.cancel()

    def _upsert_log(self, seq: int, ballot: str, req: Dict[str, Any], status: str):
        for e in self.log:
            if e["seq"] == seq:
                e["ballot"] = ballot
                e["req"] = deepcopy(req)
                e["status"] = status
                return
        self.log.append({"seq": seq, "ballot": ballot, "req": deepcopy(req), "status": status})

    def _upgrade_status(self, seq: int, status: str):
        for e in self.log:
            if e["seq"] == seq:
                if e["status"] == EntryStatus.E:
                    return
                e["status"] = status
                return

    def _get_req_by_seq(self, seq: int) -> Optional[Dict[str, Any]]:
        for e in self.log:
            if e["seq"] == seq:
                return deepcopy(e["req"])
        return None

    def _ensure_req(self, seq: int, ballot: str, req: Dict[str, Any]):
        for e in self.log:
            if e["seq"] == seq:
                if (not e.get("req")) or e["req"].get("noop"):
                    e["req"] = deepcopy(req)
                e["ballot"] = ballot
                return
        self.log.append({"seq": seq, "ballot": ballot, "req": deepcopy(req), "status": EntryStatus.A})

    async def execute_ready_prefix(self):
        progressed = False
        while True:
            target = self.last_executed_seq + 1
            entry = next((x for x in self.log if x["seq"] == target), None)
            if not entry or entry["status"] != EntryStatus.C:
                break
            req = entry["req"]
            result = "noop"
            if req and not req.get("noop"):
                s = str(req.get("s"))
                r = str(req.get("r"))
                amt = int(req.get("amt", 0))
                ok = self._apply_txn(s, r, amt)
                result = "success" if ok else "failed"
                client = str(req.get("client", ""))
                ts = int(req.get("ts", 0))
                client_port = int(req.get("client_port", CLIENT_LISTEN_PORT))
                if client:
                    self.last_reply[client] = {"ts": ts, "result": result, "ballot": self.ballot, "port": client_port}
                if self.id == self.leader_id and client:
                    await self.send_reply(client, ts, result, ballot=self.ballot, port=client_port)
            entry["status"] = EntryStatus.E
            self.last_executed_seq = target
            progressed = True
        if progressed:
            if not LOG_QUIET:
                print(f"[I] {self.name} executed up to seq {self.last_executed_seq}. DB={self.db}")
            if self.id == self.leader_id and (self.last_executed_seq - self.last_checkpoint_seq) >= CHECKPOINT_PERIOD:
                await self._broadcast_checkpoint(self.last_executed_seq)

    async def send_reply(self, client_id: str, ts: int, result: str, ballot: str, port: int):
        target_port = int(port) if port else CLIENT_LISTEN_PORT
        rp = {"type": REPLY, "from": self.id, "payload": {"ballot": ballot, "ts": ts, "client": client_id, "result": result}}
        await send_message("127.0.0.1", target_port, rp)

    def _apply_txn(self, s: str, r: str, amt: int) -> bool:
        if amt == 0 and (not s or not r):
            return True
        if s not in self.db or r not in self.db:
            return False
        if self.db[s] < amt:
            return False
        self.db[s] -= amt
        self.db[r] += amt
        return True

    async def broadcast(self, msg: Dict[str, Any]):
        tasks = []
        for nid in self.all_nodes:
            m = dict(msg)
            m["to"] = nid
            tasks.append(send_message("127.0.0.1", node_port(nid), m))
        await asyncio.gather(*tasks, return_exceptions=True)

    async def _broadcast_checkpoint(self, n: int):
        try:
            d = hashlib.sha256(json.dumps(self._db, sort_keys=True).encode()).hexdigest()
        except Exception:
            d = ""
        payload = {"n": n, "db": deepcopy(self._db), "d": d}
        ck = {"type": CHECKPOINT, "from": self.id, "payload": payload}
        await self.broadcast(ck)
        await self._apply_checkpoint(n, self._db)

    async def handle_checkpoint(self, m: Dict[str, Any]):
        p = m.get("payload", {})
        n = int(p.get("n", 0))
        db = p.get("db")
        dig = str(p.get("d", ""))
        if not isinstance(db, dict):
            return
        if n <= self.last_checkpoint_seq:
            return
        if dig:
            try:
                calc = hashlib.sha256(json.dumps(db, sort_keys=True).encode()).hexdigest()
            except Exception:
                calc = ""
            if calc and calc != dig:
                print(f"[WARN] {self.name} checkpoint digest mismatch for n={n}: got={dig} calc={calc}")
                return
        await self._apply_checkpoint(n, db)

    async def _apply_checkpoint(self, n: int, db: Dict[str, int]):
        self._db = dict(db)
        self.last_executed_seq = max(self.last_executed_seq, n)
        self.last_checkpoint_seq = n
        old_entries = [e for e in self.log if int(e.get("seq", 0)) <= n]
        if old_entries:
            self.compacted_log.extend(old_entries)
            if len(self.compacted_log) > 2000:
                self.compacted_log = self.compacted_log[-2000:]
        self.log = [e for e in self.log if int(e.get("seq", 0)) > n]
        self.accepted_per_seq = {s: v for s, v in self.accepted_per_seq.items() if s > n}
        for b in list(self.accepted_votes.keys()):
            seqmap = self.accepted_votes[b]
            for s in list(seqmap.keys()):
                if s <= n:
                    seqmap.pop(s, None)
            if not seqmap:
                self.accepted_votes.pop(b, None)
        for b in list(self.committed_broadcasted.keys()):
            seqset = self.committed_broadcasted[b]
            self.committed_broadcasted[b] = {s for s in seqset if s > n}
            if not self.committed_broadcasted[b]:
                self.committed_broadcasted.pop(b, None)
        self.next_seq = max(self.next_seq, n + 1)
        try:
            digest = hashlib.sha256(json.dumps(self._db, sort_keys=True).encode()).hexdigest()
        except Exception:
            digest = "<err>"
        # Record checkpoint in history
        self.checkpoint_history.append({"n": self.last_checkpoint_seq, "db_sha256": digest})
        if not LOG_QUIET:
            print(f"[I] {self.name} applied checkpoint to seq {self.last_checkpoint_seq} (db_sha256={digest})")

    def print_log(self):
        print(f"Node {self.id} LOG")
        for e in sorted(self.log, key=lambda x: x['seq']):
            print(f" seq={e['seq']} ballot={e['ballot']} status={e['status']} req={short_req(e['req'])}")

    def print_log_full(self):
        print(f"Node {self.id} FULL LOG")
        for e in sorted(self.log, key=lambda x: x['seq']):
            try:
                body = json.dumps(e, sort_keys=True)
            except Exception:
                body = str(e)
            print(body)

    def print_db(self):
        print(f"Node {self.id} DB: {self.db}")

    def print_status(self, seq: int):
        for e in self.log:
            if e["seq"] == seq:
                print(f"seq {seq} status={e['status']}")
                return
        print("not found")

    def print_view(self):
        print("NEW_VIEW history:")
        for i, nv in enumerate(self.new_view_history, 1):
            print(f" {i}. {nv}")

    def set_active(self, on: bool):
        self.active = on
        if on:
            self.last_leader_msg = time.time()
            self.leader_id = 0
        print(f"[I] {self.name} active={self.active}")

    def print_compacted_log(self):
        print(f"Node {self.id} COMPACTED LOG (pre-checkpoint entries)")
        if not self.compacted_log:
            print(" <empty>")
            return
        for e in sorted(self.compacted_log, key=lambda x: x['seq']):
            print(f" seq={e['seq']} ballot={e['ballot']} status={e['status']} req={short_req(e['req'])}")

    @property
    def db(self) -> Dict[str, int]:
        return self._db

    def print_checkpoint(self):
        try:
            digest = hashlib.sha256(json.dumps(self._db, sort_keys=True).encode()).hexdigest()
        except Exception:
            digest = "<err>"
        print(f"checkpoint_n={getattr(self, 'last_checkpoint_seq', 0)} db_sha256={digest}")

    def print_checkpoint_history(self):
        print("Checkpoint history:")
        if not getattr(self, 'checkpoint_history', None):
            print(" <empty>")
            return
        for rec in self.checkpoint_history:
            try:
                n = int(rec.get("n", 0))
                h = str(rec.get("db_sha256", ""))
            except Exception:
                n, h = 0, str(rec)
            print(f" n={n} db_sha256={h}")
