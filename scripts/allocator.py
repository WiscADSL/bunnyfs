"""Enable auto-configure experiments"""

from typing import List
from env import *
from driver import ufs_mkfs, uFsDriver, run_cmd


class CoreAllocator:
    def __init__(self, core_min: int, core_max: int) -> None:
        self.avail_cores = list(range(core_min, core_max + 1))

    def get_cores(self, num_cores) -> List[int]:
        if num_cores > len(self.avail_cores):
            raise RuntimeError("Fail to allocate cores")
        alloc_cores = self.avail_cores[:num_cores]
        self.avail_cores = self.avail_cores[num_cores:]
        return alloc_cores


class FileAllocator:
    def __init__(self) -> None:
        self.num_files_allocated = 0
        self.is_prepared = False

    def prepare(self) -> None:
        if self.is_prepared:
            return
        ufs_mkfs() # only mkfs when preparing
        # TODO: rewrite prepare with WriteBenchApp instead
        run_cmd(f"sudo -E {PROJ_PATH}/cfs_bench/exprs/init_mt_bench_file.py "
                f"fsp {self.num_files_allocated}",
                err_msg="Fail to prepare data")
        self.is_prepared = True

    def get_files(self, num_files) -> List[str]:
        if self.is_prepared:
            raise RuntimeError("Files preparation has done...")
        f_start = self.num_files_allocated
        self.num_files_allocated += num_files
        return [f"bench_f_{i}" for i in range(f_start, self.num_files_allocated)]

    def get_num_ops_from_sz(self, sz):
        return 2 * 1024 * 1024 // 4 * 4096 // sz  # ~2G


class WorkerAllocator:
    def __init__(self, num_workers: int) -> None:
        self.num_workers = num_workers
        self.workers = list(range(num_workers))

    def get_wid(self) -> int:
        assert len(self.workers) > 0
        # get one worker
        return self.workers.pop(0)
