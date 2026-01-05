import sys
import logging
import subprocess
import time
import os
import argparse
from sarge import run
from typing import List
from driver import uFsDriver, start_bench_coord, cleanup, mkdir, setup_spdk, reset_spdk
from bench import BaseBenchApp, ReadBenchApp
from allocator import CoreAllocator, FileAllocator, WorkerAllocator
from env import *

EXPER_DIR = f"results/microbench"
# extract first 10000 lines of nanlog (o.w. too large...)
DIGEST_NANOLOG_NUM_LINE = 10000

# we only run on one NUMA node, each has 10 cores only
app_core_allocator = CoreAllocator(1, 10)
fs_core_allocator = CoreAllocator(11, 20)
file_allocator = FileAllocator()
worker_allocator = None
ufs_driver = uFsDriver()


def post_run():
    # if compressedLog is generated, decompress it into a humen-readable log
    if os.path.exists(f"./compressedLog"):
        # Note that we only save the first 10000 lines of human-readable log...
        # I don't think any human may want to read longer log than that, but if
        # necessary, one could always to decompressor to get the full human-readable
        # log. To further make the log more human-readable, the date and some known
        # path will be filtered out so that each line won't be too long...
        run(f"{PROJ_PATH}/cfs/lib/Nanolog/runtime/decompressor decompress "
            f"./compressedLog "
            f"| head -n {DIGEST_NANOLOG_NUM_LINE} "
            f"| sed 's!{PROJ_PATH}/cfs/src/!!g' "
            "| sed 's!^...........!!g'"
            f"> {EXPER_DIR}/nanolog_digest")


def get_bench_from_string(arg: str, aid: int, use_dedi_worker: bool):
    cache_mb = None  # "c{int}"
    bw_mb = None  # "b{float}"
    num_threads = None  # "t{int}"
    sz = None  # 's{int}'
    working_set_mb = None  # "w{int}"
    zipf_theta = None  # "z{float}"

    fields = arg.split(":")
    bench = fields[0]
    for field in fields[1:]:
        if field[0] == 'c':
            cache_mb = int(field[1:])
        elif field[0] == 'b':
            bw_mb = float(field[1:])
        elif field[0] == 't':
            num_threads = int(field[1:])
        elif field[0] == 's':
            sz = int(field[1:])
        elif field[0] == 'w':
            working_set_mb = int(field[1:])
        elif field[0] == 'z':
            zipf_theta = float(field[1:])
        else:
            raise ValueError(f"Invalid field `{field}' in {arg}")

    if bench[0] == "R" and len(bench) == 4:
        assert bench[1] in {"M", "D"}
        assert bench[2] in {"S", "P"}
        assert bench[3] in {"R", "S"}
        is_cached = bench[1] == "M"
        is_shared = bench[2] == "S"
        is_rand = bench[3] == "R"
        print(f"Register App {aid}: {bench}, "
              f"cache {cache_mb} MB, bandwidth {bw_mb} MB/s, "
              f"{num_threads} threads, I/O size {sz} B, "
              f"working set {working_set_mb} MB, Zipfian theta {zipf_theta}")
        return ReadBenchApp(aid=aid, cache_mb=cache_mb, bw_mb=bw_mb,
                            num_threads=num_threads, sz=sz,
                            zipf_theta=zipf_theta, is_cached=is_cached,
                            is_shared=is_shared, is_rand=is_rand,
                            working_set=working_set_mb * 1024 * 1024 \
                                if working_set_mb is not None else None,
                            exper_dir=EXPER_DIR,
                            file_allocator=file_allocator,
                            app_core_allocator=app_core_allocator,
                            worker_allocator=worker_allocator
                            if use_dedi_worker else None)
    else:
        raise ValueError(f"Invalid bench: {bench}")


def parse_args():
    parser = argparse.ArgumentParser(
        description="Run uFS scheduler benchmark")
    parser.add_argument('-p', '--prep', action='store_true',
                        help='Prepare data')
    parser.add_argument('-w', '--workers', type=int, help='Number of workers '
                        '(0 means each app gets a dedicated worker)', default=0)
    parser.add_argument('-k', '--skip-fs', action='store_true',
                        help='Skip launching uFS; useful when launching uFS w/ GDB from cli')
    parser.add_argument('apps', metavar="bench:cache:bandwidth:#threads:sz[:ws[:zipf]]", type=str,
                        nargs='+', help='Application benchmark to run; note cache\'s unit is MB and bandwidth\'s unit is MB/s')
    return parser.parse_args()


def main(args):
    use_dedi_worker = args.workers == 0
    num_workers = args.workers
    if use_dedi_worker:
        num_workers = len(args.apps)
        global worker_allocator
        worker_allocator = WorkerAllocator(num_workers)

    bench_apps = []
    next_aid = 0
    for arg in args.apps:
        bench_apps.append(get_bench_from_string(
            arg, next_aid, use_dedi_worker))
        next_aid += 1

    cleanup(args.skip_fs)
    if not args.skip_fs:  # so we won't kill uFS launched by another script/cli
        setup_spdk()

    # prepare data files
    if args.prep:
        file_allocator.prepare()
    else:
        file_allocator.is_prepared = True

    # start uFS server
    ufs_driver.start(num_apps=len(bench_apps),
                     worker_cores=fs_core_allocator.get_cores(num_workers),
                     worker_app_configs=[bench_app.config
                                         for bench_app in bench_apps
                                         if bench_app.config is not None],
                     log_dir=EXPER_DIR,
                     skip_fs=args.skip_fs)

    start_bench_coord(sum(bench_app.num_threads for bench_app in bench_apps),
                      EXPER_DIR)

    # run apps
    apps = [bench_app.run(ufs_driver) for bench_app in bench_apps]
    for a in apps:
        a.wait()
    time.sleep(1)

    # stop fsp server
    ufs_driver.stop()

    if not args.skip_fs:
        reset_spdk()
    cleanup(args.skip_fs)


if __name__ == '__main__':
    logging.root.setLevel(logging.INFO)
    mkdir(EXPER_DIR)
    args = parse_args()
    main(args)
