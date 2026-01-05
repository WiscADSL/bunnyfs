#!/usr/bin/env python3
from pathlib import Path

import time
from utils import run_cmd

EXIT_FNAME = Path("/tmp/ufs_exit")
READY_FNAME = Path("/tmp/ufs_ready")


def ufs_cleanup() -> None:
    run_cmd("sudo killall -9 fsMain", silent=True, err_panic=False)
    run_cmd("sudo killall -9 bench", silent=True, err_panic=False)
    run_cmd("sudo killall -9 prep", silent=True, err_panic=False)
    run_cmd("sudo killall -9 tree", silent=True, err_panic=False)
    run_cmd("sudo killall -9 migration_test", silent=True, err_panic=False)
    run_cmd("sudo killall -9 tests", silent=True, err_panic=False)
    run_cmd("sudo killall -9 cfs_bench_coordinator", silent=True, err_panic=False)
    run_cmd("sudo rm -f /ufs-*", silent=True, err_panic=False)
    run_cmd("sudo rm -f ./compressedLog", silent=True, err_panic=False)
    run_cmd("sudo rm -f /dev/shm/*", silent=True, err_panic=False)
    run_cmd(f"sudo rm -f {EXIT_FNAME}", silent=True, err_panic=False)
    run_cmd(f"sudo rm -f {READY_FNAME}", silent=True, err_panic=False)
    time.sleep(1)  # give some time for cleanup to take effect


def main():
    ufs_cleanup()


if __name__ == "__main__":
    main()
