#!/usr/bin/env python3

from ufs_run import run_ufs, get_ufs_cmd, stop_ufs
from utils import run_cmd, build_bench, PROJ_PATH

if __name__ == "__main__":
    build_bench(target="tree")

    fs_proc = run_ufs(get_ufs_cmd(num_workers=1, num_apps=1,
                      total_cache_mb=1024, total_bandwidth_mbps=2048, core_ids=[17]))
    run_cmd(str(PROJ_PATH / "bench" / "build-release" / "tree"))
    stop_ufs(fs_proc)
