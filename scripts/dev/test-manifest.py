#!/usr/bin/env python3
"""Balance Go top-level tests using recorded durations, retaining every name."""
import argparse
import json
import pathlib
import re
import sys


def manifest(names, count, durations):
    if count < 1 or len(names) != len(set(names)):
        raise ValueError("positive shard count and unique test names required")
    if any(not re.fullmatch(r"(?:Test|Example|Fuzz)\w*", name) for name in names):
        raise ValueError("unexpected Go test name")
    shards = [{"tests": [], "estimated_seconds": 0.0} for _ in range(count)]
    for name in sorted(names, key=lambda name: (-durations.get(name, 1.0), name)):
        shard = min(shards, key=lambda shard: shard["estimated_seconds"])
        shard["tests"].append(name)
        shard["estimated_seconds"] += durations.get(name, 1.0)
    assigned = [name for shard in shards for name in shard["tests"]]
    if sorted(assigned) != sorted(names):
        raise ValueError("shards must select every test exactly once")
    for shard in shards:
        shard["tests"].sort()
        shard["estimated_seconds"] = round(shard["estimated_seconds"], 3)
    return {"test_count": len(names), "unmeasured_count": sum(n not in durations for n in names), "shards": shards}


def timings(paths):
    result = {}
    for path in paths:
        with open(path) as source:
            for line in source:
                try:
                    event = json.loads(line)
                except json.JSONDecodeError:
                    continue
                name = event.get("Test", "")
                if event.get("Action") not in ("pass", "fail") or not name or "/" in name:
                    continue
                package = "./" + event["Package"].split("Auto-Commit-Daemon/", 1)[-1]
                bucket = result.setdefault(package, {})
                bucket[name] = max(bucket.get(name, 0), event.get("Elapsed", 0.0), 0.001)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    balance = commands.add_parser("balance")
    balance.add_argument("package")
    balance.add_argument("count", type=int)
    balance.add_argument("names", type=pathlib.Path)
    balance.add_argument("--timings", type=pathlib.Path, default=pathlib.Path(__file__).with_name("test-timings.json"))
    collect = commands.add_parser("timings")
    collect.add_argument("paths", nargs="+")
    pattern = commands.add_parser("pattern")
    pattern.add_argument("manifest", type=pathlib.Path)
    pattern.add_argument("index", type=int)
    args = parser.parse_args()
    if args.command == "timings":
        value = timings(args.paths)
    elif args.command == "balance":
        durations = json.loads(args.timings.read_text()) if args.timings.exists() else {}
        value = manifest(args.names.read_text().splitlines(), args.count, durations.get(args.package, {}))
        value["package"] = args.package
    else:
        value = json.loads(args.manifest.read_text())
        names = value["shards"][args.index]["tests"]
        print("^(" + "|".join(names) + ")$" if names else "^$")
        return
    json.dump(value, sys.stdout, indent=2, sort_keys=True)
    print()


if __name__ == "__main__":
    main()
