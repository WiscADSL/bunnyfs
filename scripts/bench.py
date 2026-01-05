import logging
import subprocess
from typing import Optional
from env import *


class BaseBenchApp:
    def __init__(self, aid: int, cache_mb: int, bw_mb: float, num_threads: int, sz: int):
        self.aid = aid
        self.cache_mb = cache_mb
        self.bw_mb = bw_mb
        self.num_threads = int(num_threads)
        self.sz = sz
        self.need_data_prep = False
        assert sz % 4096 == 0

    @property
    def log_name(self) -> str:
        return f"aid={self.aid}-cache={self.cache_mb}-bw={self.bw_mb}-threads={self.num_threads}-sz={self.sz}.log"


class ReadBenchApp(BaseBenchApp):
    def __init__(self, *, aid: int, cache_mb: int, bw_mb: float,
                 num_threads: int, sz: int, is_cached: bool,
                 is_shared: bool, is_rand: bool, working_set: Optional[int],
                 zipf_theta: Optional[float], exper_dir: str, file_allocator,
                 app_core_allocator, worker_allocator):
        super().__init__(aid=aid, cache_mb=cache_mb,
                         bw_mb=bw_mb, num_threads=num_threads, sz=sz)
        self.is_cached = is_cached  # else disk IO
        self.is_shared = is_shared  # else private
        self.is_rand = is_rand  # else seq
        self.need_data_prep = True
        self.is_no_overlap = False  # we want some cache hit
        self.num_threads = num_threads
        self.files = file_allocator.get_files(
            1 if self.is_shared else self.num_threads)
        self.log_path = f"{exper_dir}/{self.log_name}"
        self.num_ops = file_allocator.get_num_ops_from_sz(self.sz)
        if self.is_shared:
            self.num_ops /= self.num_threads
        self.cores = app_core_allocator.get_cores(self.num_threads)
        self.wid = worker_allocator.get_wid()
        self.working_set = working_set
        self.zipf_theta = zipf_theta

    @property
    def bench(self) -> str:
        return f"R{'M' if self.is_cached else 'D'}" \
            f"{'S' if self.is_shared else 'P'}" \
            f"{'R' if self.is_rand else 'S'}"

    @property
    def config(self) -> Optional[str]:
        return f"w{self.wid}-a{self.aid}:c{self.cache_mb}:b{self.bw_mb:g}:p1" \
            if self.wid is not None else None

    def run(self, ufs_driver) -> subprocess.Popen:
        self.bench_args = {
            "--benchmarks=": f"c{'r' if self.is_rand else 's'}read"
            if self.is_cached else f"{'r' if self.is_rand else 'seq'}read",
            "--rw_align_bytes=": self.sz,  # self.sz must be multiple of 4096
            "--rand_no_overlap=": 1 if self.is_no_overlap else None,
            "--in_mem_file_size=": 64 * 1024 if self.is_cached else None,
            "--threads=": self.num_threads,
            "--value_size=": self.sz,
            "--histogram=": 1,
            "--numop=": self.num_ops,
            "--wid=": self.wid,
            "--fs_worker_key_list=": ufs_driver.get_app_shm_names(self.aid),
            "--fname=": self.files[0] if self.is_shared else None,
            "--core_ids=": ",".join(f"{c}" for c in self.cores),
            "--dirfile_name_prefix=": None,  # only used in META workload
            "--flist=": ",".join(self.files) if not self.is_shared else None,
            "--working-set=": self.working_set,
            "--zipf_theta=": self.zipf_theta if self.zipf_theta is not None else None,
        }

        # dump command-line args
        self.app_cmd = f"{PROJ_PATH}/cfs_bench/build/bins/cfs_bench " \
            f"{' '.join(f'{k}{v}' for k, v in self.bench_args.items() if v is not None)} " \
            f">> {self.log_path}"
        with open(self.log_path, "w") as f:
            f.write(f"=== Command ===\n{self.app_cmd}\n===============\n")
        logging.info(f"Running command `{self.app_cmd}`")
        return subprocess.Popen(self.app_cmd, shell=True)


class WriteBenchApp(BaseBenchApp):
    pass


class MetaBenchApp(BaseBenchApp):
    pass


class StatBenchApp(MetaBenchApp):
    pass
