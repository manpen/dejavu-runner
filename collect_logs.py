#!/usr/bin/env python3
import json
from argparse import ArgumentParser
from pathlib import Path
from dejavu_parser import load_dejavu_log

if __name__ == "__main__":
    p = ArgumentParser()

    p.add_argument("-i", "--input-dir", required=True, type=Path)
    p.add_argument("-o", "--output-dir", required=False, type=Path)

    args = p.parse_args()

    # check input
    assert args.input_dir.is_dir()

    # compute output fir
    output = args.output_dir
    if output is None:
        output = args.input_dir / "collected.json"

    log_files = list(args.input_dir.rglob("*.log"))
    print(f"Found {len(log_files)} log files")

    logs = []
    for log_file in log_files:
        try:
            with open(log_file, "r") as f:
                log = json.load(f)

            dejavu_log = load_dejavu_log(log["outputs"]["stderr"])
        except Exception as e:
            print(f"Skip file {log_file} due to error: {e}")
            continue

        if dejavu_log is None or "solve_time" not in dejavu_log:
            print(f"Incomplete run {log_file}")
        else:
            log["exec"] = dejavu_log

        logs.append(log)

    output.parent.mkdir(parents=True, exist_ok=True)
    with open(output, "w") as fout:
        json.dump(logs, fout, indent=1)
