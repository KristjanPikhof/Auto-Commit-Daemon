#!/usr/bin/env python3
"""Render package summaries and failed Go tests from retained JSON events."""
import json
import sys


def render(path):
    records = []
    with open(path) as source:
        for line in source:
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError:
                print(line, end="")
    failed = {
        (record.get("Package"), record.get("Test", "").split("/")[0])
        for record in records
        if record.get("Action") == "fail"
    }
    for event in records:
        test = event.get("Test", "")
        if event.get("Action") != "output":
            continue
        if test and (event.get("Package"), test.split("/")[0]) not in failed:
            continue
        output = event.get("Output", "")
        if not output.startswith(("=== RUN", "=== PAUSE", "=== CONT", "=== NAME")):
            print(output, end="")


if __name__ == "__main__":
    render(sys.argv[1])
