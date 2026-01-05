#!/usr/bin/env python3
import argparse
import logging

from ufs_cleanup import ufs_cleanup
from utils import PROJ_PATH, run_cmd


def get_ufs_build_dir():
    return PROJ_PATH / "cfs" / "build"


def build_dependencies():
    """Check if required dependencies are built, build them if missing."""
    cfs_lib_dir = PROJ_PATH / "cfs" / "lib"
    missing_deps = []

    # Check config4cpp
    if not (cfs_lib_dir / "config4cpp" / "lib" / "libconfig4cpp.a").exists():
        missing_deps.append("config4cpp")

    # Check SPDK
    spdk_lib = cfs_lib_dir / "spdk" / "build" / "lib"
    if not spdk_lib.exists() or not any(spdk_lib.glob("*.a")):
        missing_deps.append("SPDK")

    # Check TBB
    tbb_build = cfs_lib_dir / "tbb" / "build"
    if not tbb_build.exists() or not any(tbb_build.glob("linux_*_release/libtbb.so*")):
        missing_deps.append("TBB")

    # Check Nanolog
    if not (cfs_lib_dir / "Nanolog" / "runtime" / "libNanoLog.a").exists():
        missing_deps.append("Nanolog")

    if missing_deps:
        logging.warning(f"Missing dependencies: {', '.join(missing_deps)}")
        logging.info("Building dependencies with 'make -C cfs/lib'...")
        logging.info("This may take several minutes on first build...")

        run_cmd(
            f"make -C {PROJ_PATH}/cfs/lib -j `nproc`",
            ok_msg="Successfully built all dependencies!",
            err_msg="Failed to build dependencies. Please check the output above.",
        )


def ufs_configure_then_build(
    sched: bool, leveldb: bool, fine_grained: bool, high_freq: bool
):
    ufs_cleanup()  # leftover processes may cause compilation out-of-memory

    build_dependencies()  # build dependencies if missing

    real_build_dir = PROJ_PATH / "cfs" / (
        f"build_"
        f"leveldb={leveldb}_"
        f"sched={sched}_"
        f"finegrained={fine_grained}_"
        f"highfreq={high_freq}"
    )
    flags = " ".join([
        "-DCMAKE_BUILD_TYPE=Release",
        "-DCFS_TRACE_ENABLE=ON",
        f"-DCFS_DISK_LAYOUT_LEVELDB={'ON' if leveldb else 'OFF'}",
        f"-DCFS_SCHED_TYPE={'DO_SCHED' if sched else 'NOOP_SCHED'}",
        f"-DALLOC_FINE_GRAINED={'ON' if fine_grained else 'OFF'}",
        f"-DALLOC_HIGH_FREQ={'ON' if high_freq else 'OFF'}",
        "-DCFS_DISK_SIZE=858993459200UL",
    ])

    run_cmd(f"cmake -S {PROJ_PATH}/cfs -B {real_build_dir} {flags}",
            ok_msg="CMake configure uFS successfully!",
            err_msg="Fail to configure uFS!")

    # make a symlink to the real build dir, so that we can easily switch between
    # different build configurations without fully rebuilding uFS.
    sym_build_dir = get_ufs_build_dir()
    run_cmd(f"rm -rf {sym_build_dir}")
    sym_build_dir.symlink_to(real_build_dir, target_is_directory=True)

    # These are all the targets we need to build to run the experiments.
    targets = ["fsMain", "fsProcOfflineCheckpointer", "testRWFsUtil", "testAppCli"]
    run_cmd(
        f"cmake "
        f"--build {sym_build_dir} "
        f"--target {' '.join(targets)} "
        f"--parallel `nproc`",
        ok_msg="Build uFS successfully!",
        err_msg="Fail to build uFS!",
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--sched", action="store_true")
    parser.add_argument("--no-sched", action="store_false", dest="sched")
    parser.add_argument("--leveldb", action="store_true")
    parser.add_argument("--no-leveldb", action="store_false", dest="leveldb")
    parser.add_argument("--fine-grained", action="store_true")
    parser.add_argument("--no-fine-grained",
                        action="store_false", dest="fine-grained")
    parser.add_argument("--high-freq", action="store_true")
    parser.add_argument("--no-high-freq",
                        action="store_false", dest="high-freq")
    parser.set_defaults(sched=True, leveldb=False,
                        fine_grained=False, high_freq=False)

    args = parser.parse_args()
    ufs_configure_then_build(sched=args.sched,
                             leveldb=args.leveldb,
                             fine_grained=args.fine_grained,
                             high_freq=args.high_freq)
