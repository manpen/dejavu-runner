#!/usr/bin/env python3
import argparse
import datetime
import subprocess
import multiprocessing as mp
from time import time
from typing import Tuple, List
from pathlib import Path
import re
from shutil import copyfile, copymode
import json

RUNNER = Path(__file__).parent / "runner_dejavu.py"
assert RUNNER.is_file()


def estimate_cores() -> int:
    """Estimates the number of cores available on a ucloud machine"""
    try:
        a = int(open("/sys/fs/cgroup/cpu/cpu.cfs_quota_us").read())
        b = int(open("/sys/fs/cgroup/cpu/cpu.cfs_period_us").read())
        return a // b
    except Exception:
        return 1


def estimate_cores_and_memory() -> Tuple[int, int]:
    """Estimates the number of cores and memory (in GB) available on an ucloud machine"""
    cores = estimate_cores()
    mem = 6 * cores  # on ucloud, each core comes with 6GB RAM
    return cores, mem


def load_instances(path: Path) -> List[Path]:
    """Loads instances from a path"""
    instances = []
    with open(path, "r") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            instance = Path(line)
            assert instance.exists()
            instances.append(instance)
    return instances


def execute(
    runner: Path,
    instance: Path,
    output: Path,
    dejavu: Path,
    dargs: List[str],
    timeout: float,
    mem: float,
):
    cmd = [runner, "--clean"]
    cmd += ["-o", output]
    cmd += ["-d", dejavu]
    cmd += ["-t", timeout]
    cmd += ["-m", mem]
    cmd += ["-i", instance]
    if dargs:
        if dargs[0] != "--":
            cmd.append("--")
        cmd += dargs

    cmd = [str(s) for s in cmd]

    print("Executing ", " ".join(cmd))
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE)
    try:
        stdout, _ = proc.communicate(timeout=timeout + 5)
    except subprocess.TimeoutExpired:
        proc.kill()
        stdout, _ = proc.communicate()

    stdout = stdout.decode("ascii")

    if "#mem out" in stdout:
        print(f"Mem limit reached for {instance}")
        return instance

    return None


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("-i", "--instances", type=Path, required=True, help="Set file")
    p.add_argument("-d", "--dejavu", type=Path, required=True, help="Dejavu solver")
    p.add_argument("-o", "--output", type=Path, required=True, help="Output directory")
    p.add_argument("-t", "--timeout", type=float, default=120, help="Timeout in sec")
    p.add_argument("-m", "--mem", type=float, default=1.0, help="Init Mem lim in GB")
    p.add_argument("-M", "--mem-max", type=float, help="Max mem limit in GB")
    p.add_argument("-j", "--jobs", type=int, help="Number of parallel jobs")
    p.add_argument("-l", "--label", type=str, help="Label for jobs")
    p.add_argument("child_args", nargs=argparse.REMAINDER)

    args = p.parse_args()
    assert args.dejavu.is_file()
    assert not args.output.exists() or args.output.is_dir()

    instances = load_instances(args.instances)
    print(f"Found {len(instances)} instances")

    if args.label is None:
        label = args.instances.stem + "_".join(args.child_args)
    else:
        label = args.label

    timestamp = str(datetime.datetime.now()).replace("-", "").replace(":", "")
    dirname = re.sub(
        r"[^\w\d\-\.]",
        "_",
        timestamp + "_" + label,
    )
    output_dir = args.output / dirname
    output_dir.mkdir(parents=True, exist_ok=True)

    # copy runner, dejavu and instances
    dejavu_copy = output_dir / "dejavu"
    runner_copy = output_dir / "runner_dejavu.py"

    def copy_with_mode(s, n=None):
        if n is None:
            d = output_dir / s.name
        elif isinstance(n, Path):
            d = n
        else:
            d = output_dir / n

        copyfile(s, d)
        copymode(s, d)

    copy_with_mode(Path(__file__))
    copy_with_mode(Path(__file__).parent / "dejavu_parser.py")
    copy_with_mode(Path(__file__).parent / "runner_dejavu.py", runner_copy)
    copy_with_mode(args.dejavu, dejavu_copy)
    copy_with_mode(args.instances, "instances")

    host_cores, host_mem = estimate_cores_and_memory()

    if args.jobs:
        host_cores = args.jobs

    mem_limit = args.mem
    mem_max_limit = args.mem_max
    if mem_max_limit is None:
        mem_max_limit = host_mem

    while instances:
        if mem_limit > mem_max_limit:
            print(f"Memory limit {mem_limit} exceeds --mem-max {mem_max_limit}")
            break

        tasks = [
            (
                runner_copy,
                inst,
                output_dir,
                dejavu_copy,
                args.child_args,
                args.timeout,
                mem_limit,
            )
            for inst in instances
        ]

        parallel_jobs = min(int(host_mem / mem_limit), host_cores)
        if parallel_jobs < 1:
            print("Not enough host memory")
            break

        parallel_jobs = min(parallel_jobs, len(tasks))

        if parallel_jobs == 1 and mem_limit < mem_max_limit:
            print("One core remaining -> increase mem limit to max")
            mem_limit = mem_max_limit

        print(
            f"Start {len(tasks)} tasks with {parallel_jobs} parallel jobs and a mem limit of {mem_limit} GiB"
        )
        start = time()
        with mp.Pool(processes=parallel_jobs) as pool:
            result = pool.starmap(execute, tasks)

        remaining = [p for p in result if p is not None]
        print(
            f"Round took {time() - start:.1f}s. There are {len(remaining)} remaining tasks"
        )

        instances = remaining
        mem_limit *= 2

    if instances:
        print(f"There are {len(instances)} instances left")

    jsons = output_dir.glob("job_*.json")
    collected = []
    for j in jsons:
        with open(j) as f:
            collected.append(json.load(f))
    with open(output_dir / "collected.json", "w") as f:
        json.dump(collected, f)
