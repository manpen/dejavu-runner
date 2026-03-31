#!/usr/bin/env python3
import json
import sys
import time
import ctypes
import signal
import resource
import subprocess
import threading

import argparse
from argparse import ArgumentParser
from pathlib import Path


def set_pdeathsig() -> None:
    """
    Called in the child process (preexec_fn).
    Ask the kernel to send SIGKILL to this process when its parent dies.
    """
    try:
        PR_SET_PDEATHSIG = 1  # from <sys/prctl.h>
        _libc = ctypes.CDLL("libc.so.6", use_errno=True)

        ret = _libc.prctl(PR_SET_PDEATHSIG, signal.SIGKILL, 0, 0, 0)
        if ret != 0:
            # Non-fatal: ignore
            pass

    except Exception:
        pass


def install_parent_signal_handlers(proc: subprocess.Popen, result: dict) -> None:
    """
    In the parent: forward SIGTERM / SIGINT to the child, then re-raise.
    SIGKILL cannot be caught, but mechanism #1 (prctl) already covers it.
    """

    def forward(signum, _frame):
        try:
            proc.kill()
        except ProcessLookupError:
            pass

        result["signal"] = signum

    signal.signal(signal.SIGTERM, forward)
    signal.signal(signal.SIGINT, forward)


def monitor_memory_usage_and_kill(
    proc: subprocess.Popen, stop_event: threading.Event, max_rss: int, result: dict
) -> None:
    """
    Poll the child's RSS every 250 ms.
    Kill it if it exceeds MAX_RSS_BYTES; record the peak either way.
    """

    def _rss_of(pid: int) -> int:
        """Return current RSS in bytes for *pid* by reading /proc, or 0 on error."""
        try:
            with open(f"/proc/{pid}/status") as fh:
                for line in fh:
                    if line.startswith("VmRSS:"):
                        # VmRSS is reported in kB
                        return int(line.split()[1]) * 1024
        except OSError:
            pass
        return 0

    while not stop_event.is_set():
        rss = _rss_of(proc.pid)
        if rss > max_rss:
            proc.kill()
            result["memed_out"] = True
            return
        stop_event.wait(0.25)


def main(
    dejavu_path: Path,
    instance_path: Path,
    output_base_path: Path,
    timeout: float,
    memory_out_gb: float,
    dejavu_args: list[str],
    capture_stdout=False,
    capture_stderr=True,
) -> None:
    logout_path = output_base_path.parent / (output_base_path.name + ".log")
    stdout_path = output_base_path.parent / (output_base_path.name + ".stdout")
    stderr_path = output_base_path.parent / (output_base_path.name + ".stderr")

    result = {
        "args": {
            "dejavu_path": str(dejavu_path),
            "dejavu_args": str(dejavu_args),
            "instance_path": str(instance_path),
            "timeout_sec": timeout,
            "memory_out_gb": memory_out_gb,
        },
        "outputs": {},
        "timed_out": False,
        "memed_out": False,
        "signal": None,
    }

    stdout_file = subprocess.DEVNULL
    if capture_stdout:
        result["outputs"]["stdout"] = str(stdout_path)
        stdout_file = open(stdout_path, "w")

    stderr_file = subprocess.DEVNULL
    if capture_stderr:
        result["outputs"]["stderr"] = str(stderr_path)
        stderr_file = open(stderr_path, "w")

    cmd = [str(dejavu_path)] + dejavu_args + [str(instance_path)]
    print("Executing cmd:", " ".join(cmd))

    proc = subprocess.Popen(
        cmd,
        stdout=stdout_file,
        stderr=stderr_file,
        preexec_fn=set_pdeathsig,  # kernel-level death signal
    )
    install_parent_signal_handlers(proc, result)

    stop_event = threading.Event()
    monitor_thread = threading.Thread(
        target=monitor_memory_usage_and_kill,
        args=(proc, stop_event, int(memory_out_gb * 2**30), result),
        daemon=True,
    )
    monitor_thread.start()

    wall_start = time.monotonic()
    result["timed_out"] = False

    try:
        proc.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        result["timed_out"] = True
        proc.kill()
        proc.wait()
    finally:
        wall_elapsed = time.monotonic() - wall_start
        stop_event.set()
        monitor_thread.join()

    usage = resource.getrusage(resource.RUSAGE_CHILDREN)
    result["exitcode"] = proc.returncode
    result["usage"] = {
        "walltime": wall_elapsed,
        "usertime": usage.ru_utime,
        "systime": usage.ru_stime,
        "maxrss": usage.ru_maxrss * 1024,
    }

    with open(logout_path, "w") as logout:
        json.dump(result, logout)

    # no need to close files: we exit anyhow ...

    # Exit with the child's code when possible, else 1
    sys.exit(proc.returncode if proc.returncode >= 0 else 1)


if __name__ == "__main__":
    p = ArgumentParser()

    p.add_argument(
        "-i",
        "--input",
        type=Path,
        required=True,
        help="Input instance relative to input base",
    )
    p.add_argument("-I", "--input-base", type=Path, required=True, help="Input base")
    p.add_argument("-d", "--dejavu", type=Path, required=True, help="Dejavu solver")
    p.add_argument("-o", "--output", type=Path, required=True, help="Output directory")
    p.add_argument("-t", "--timeout", type=float, default=120, help="Timeout in sec")
    p.add_argument("-m", "--memory", type=float, default=10.0, help="Mem lim in GB")
    p.add_argument("child_args", nargs=argparse.REMAINDER)

    args = p.parse_args()

    assert args.input_base.is_dir()
    assert args.dejavu.is_file()

    input_path = args.input_base / args.input
    assert input_path.is_file(), f"{input_path} is not a file"

    output_base_path = args.output / args.input
    output_base_path.parent.mkdir(parents=True, exist_ok=True)

    child_args = (
        args.child_args[1:]
        if args.child_args and args.child_args[0] == "--"
        else args.child_args
    )

    main(
        dejavu_path=args.dejavu,
        instance_path=input_path,
        output_base_path=output_base_path,
        timeout=args.timeout,
        memory_out_gb=args.memory,
        dejavu_args=child_args,
    )
