from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

REQUEST = "REQUEST"
REPLY = "REPLY"
PREPARE = "PREPARE"
ACK = "ACK"
ACCEPT = "ACCEPT"
ACCEPTED = "ACCEPTED"
COMMIT = "COMMIT"
NEW_VIEW = "NEW_VIEW"
CHECKPOINT = "CHECKPOINT"
CONTROL = "CONTROL"
HEARTBEAT = "HEARTBEAT"
REDIRECT = "REDIRECT"
CLIENT_LEADER = "CLIENT_LEADER"

NOOP_REQ: Dict[str, Any] = {"noop": True}

CLIENT_LISTEN_PORT = 6000
ALL_NODE_IDS = [1, 2, 3, 4, 5]

CHECKPOINT_PERIOD = 10  # number of executed requests between checkpoints

INITIAL_ACCOUNTS = list("ABCDEFGHIJ")
INITIAL_BALANCE = 10


def node_port(nid: int) -> int:
    return 5000 + nid  # nodes 1..5 => ports 5001..5005

def parse_ballot(b: str) -> Tuple[int, int]:
    try:
        t, n = b.split(":")
        return (int(t), int(n))
    except Exception:
        return (0, 0)

def ballot_str(t: int, node: int) -> str:
    return f"{t}:{node}"

def ballot_gt(a: str, b: str) -> bool:
    at, an = parse_ballot(a)
    bt, bn = parse_ballot(b)
    return (at, an) > (bt, bn)

def ballot_ge(a: str, b: str) -> bool:
    at, an = parse_ballot(a)
    bt, bn = parse_ballot(b)
    return (at, an) >= (bt, bn)

def max_ballot(a: str, b: str) -> str:
    at, an = parse_ballot(a)
    bt, bn = parse_ballot(b)
    return a if (at, an) >= (bt, bn) else b

def short_req(req: Dict[str, Any]) -> str:
    if not req:
        return "{}"
    if req.get("noop"):
        return "noop"
    return f"{req.get('s','?')}->{req.get('r','?')}:{req.get('amt','?')} ts={req.get('ts','?')}"

@dataclass
class ScenarioStep:
    kind: str
    txn: Optional[Dict[str, Any]] = None
    name: str = ""

    @property
    def is_event(self) -> bool:
        return self.kind == "event"

@dataclass
class ScenarioSet:
    set_id: int
    live_nodes: List[int]
    steps: List[ScenarioStep]

class EntryStatus:
    X, A, C, E = "X", "A", "C", "E"

# Suppressing for benchmarking
LOG_QUIET = False
def set_log_quiet(on: bool) -> None:
    global LOG_QUIET
    LOG_QUIET = on
