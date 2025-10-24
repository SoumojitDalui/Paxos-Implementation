import argparse, asyncio, os, shlex, subprocess, sys
from client import Client
from controller import run_csv_interactive
from node import Node
from benchmark import run_smallbank_benchmark
from common import set_log_quiet


async def node_main(nid: int):
    n = Node(nid)

    async def repl():
        loop = asyncio.get_running_loop()
        while True:
            line = await loop.run_in_executor(None, sys.stdin.readline)
            if not line:
                break
            cmd = line.strip()
            if cmd == "printLog":
                n.print_log()
            elif cmd == "printFullLog":
                n.print_log_full()
            elif cmd == "printDB":
                n.print_db()
            elif cmd == "printLeader":
                if n.leader_id:
                    print(f"leader=n{n.leader_id}, my_ballot={n.ballot}, max_ballot_seen={n.max_ballot_seen}")
                else:
                    print("leader=unknown (no election yet)")
            elif cmd.startswith("printStatus "):
                try:
                    n.print_status(int(cmd.split()[1]))
                except Exception:
                    print("usage: printStatus <seq>")
            elif cmd == "printView":
                n.print_view()
            elif cmd == "printCheckpoint":
                n.print_checkpoint()
            elif cmd == "printCheckpointHistory":
                n.print_checkpoint_history()
            elif cmd == "printCompactedLog":
                n.print_compacted_log()
            elif cmd in ("quit", "exit"):
                os._exit(0)
            else:
                print("commands: printLog | printFullLog | printDB | printLeader | printStatus <seq> | printView | printCheckpoint | printCheckpointHistory | printCompactedLog | quit")

    await asyncio.gather(n.start(), repl())


async def client_main():
    c = Client()
    await c.start()
    print("Enter transactions like: A B 5  (Ctrl+C to exit)")
    loop = asyncio.get_running_loop()
    while True:
        line = await loop.run_in_executor(None, sys.stdin.readline)
        if not line:
            break
        parts = line.strip().split()
        if len(parts) != 3:
            print("format: S R AMT")
            continue
        s, r, amt = parts[0], parts[1], int(parts[2])
        await c.send_txn(s, r, amt)


def _spawn_terminals(csv_path: str):
    repo_root = os.path.dirname(os.path.abspath(__file__))
    py = os.path.join(repo_root, "paxos.py")

    def open_terminal(command: str):
        try:
            esc = command.replace("\\", "\\\\").replace('"', '\\"')
            script = f'tell application "Terminal" to do script "{esc}"'
            subprocess.run(["osascript", "-e", script], check=True)
        except Exception as e:
            print(f"[WARN] Failed to open Terminal: {e}")

    # Creates 5 node terminals
    for nid in [1, 2, 3, 4, 5]:
        cmd = f"cd {shlex.quote(repo_root)}; python3 {shlex.quote(py)} --mode node --id {nid}"
        open_terminal(cmd)

    # Creates one CSV runner terminal (clients orchestrator), skipping node startup
    csv = csv_path or os.path.join(repo_root, "CSE535-F25-Project-1-Testcases.csv")
    cmd_clients = (
        f"cd {shlex.quote(repo_root)}; "
        f"python3 {shlex.quote(py)} --mode csv --csv {shlex.quote(csv)} --no-start-nodes"
    )
    open_terminal(cmd_clients)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--mode", choices=["node", "client", "csv", "bench"], default="csv")
    p.add_argument("--id", type=int, default=1, help="node id for --mode node")
    p.add_argument("--csv", type=str, default="CSE535-F25-Project-1-Testcases.csv", help="CSV file for --mode csv")
    p.add_argument("--spawn-terminals", action="store_true", help="Open 5 node terminals + 1 clients terminal")
    p.add_argument("--no-start-nodes", action="store_true", help="In CSV mode, do not start nodes in-process")
    p.add_argument("--quiet", action="store_true", help="Reduce logging in CSV mode")
    p.add_argument("--reset-between-sets", action="store_true", help="Reset node state between sets (default: preserve)")

    p.add_argument("--ops", type=int, default=20, help="Benchmark: total operations")
    p.add_argument("--concurrency", type=int, default=30, help="Benchmark: concurrent workers")
    p.add_argument("--zipf-s", type=float, default=0.9, help="Benchmark: Zipf skew parameter")
    args = p.parse_args()
    try:
        if args.spawn_terminals:
            _spawn_terminals(args.csv)
            return
        if args.mode == "node":
            asyncio.run(node_main(args.id))
        elif args.mode == "client":
            asyncio.run(client_main())
        elif args.mode == "bench":
            set_log_quiet(True)
            asyncio.run(run_smallbank_benchmark(ops=args.ops, concurrency=args.concurrency, zipf_s=args.zipf_s))
        else:
            if not args.csv:
                print("provide --csv path")
                sys.exit(1)
            if args.quiet:
                set_log_quiet(True)
            asyncio.run(run_csv_interactive(args.csv, start_nodes=not args.no_start_nodes, preserve_state=not args.reset_between_sets))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
