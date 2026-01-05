"""
This file contains general utility functions.
"""

import logging
import os
import signal
import subprocess
from pathlib import Path
from typing import Union

PROJ_PATH = Path(__file__).absolute().parent.parent

logging.basicConfig(format='[%(levelname)s] %(message)s', level=logging.INFO)


def check_rc(ret: int,
             *,
             ok_msg: str = None,
             err_msg: str = None,
             err_panic: bool = True) -> None:
    # check whether the return code is zero
    if ret == 0:
        if ok_msg is not None:
            logging.info(f"\x1b[32m{ok_msg}\x1b[0m")
    else:
        if err_panic:
            raise RuntimeError(err_msg)
        if err_msg is not None:
            logging.warning(f"\x1b[31m{err_msg}\x1b[0m")


def run_cmd(cmd: str,
            *,
            ok_msg: str = None,
            err_msg: str = None,
            err_panic: bool = True,
            silent: bool = False) -> None:
    # run a command and check return code
    logging.info(f"Run command: \x1b[34m{cmd}\x1b[0m")
    ret = subprocess.call(cmd,
                          shell=True,
                          stdout=subprocess.DEVNULL if silent else None,
                          stderr=subprocess.DEVNULL if silent else None)
    check_rc(ret, err_msg=err_msg, ok_msg=ok_msg, err_panic=err_panic)


def build_bench(target="bench"):
    run_cmd(
        f"make -C {PROJ_PATH}/bench release TARGETS={target}",
        err_msg=f"Fail to build {target}!",
        ok_msg=f"Successfully built {target}!"
    )
    return PROJ_PATH / "bench" / "build-release" / target


def run_bench(
    spec_fname: Union[str, Path],
    output_dir: Union[str, Path],
    ufs_cmd: str,
    prep: bool = False,
    timeout: int = 90,
):
    from ufs_run import run_ufs, stop_ufs

    bench_path = build_bench("prep" if prep else "bench")

    fs_proc = run_ufs(cmd=ufs_cmd, output_dir=output_dir)

    cmd = f"{bench_path} {spec_fname} -o {output_dir}"
    logging.info(f"Run command: \x1b[34m{cmd}\x1b[0m")
    proc = subprocess.Popen(cmd, shell=True)
    try:
        ret = proc.wait(timeout=timeout)
        logging.info("=== Bench finishes ===")
    except subprocess.TimeoutExpired:
        proc.send_signal(signal.SIGKILL)
        logging.warning(
            f"=== Bench does not finish in {timeout}s; will kill it ===")
        ret = proc.wait()
    check_rc(ret, err_msg="Bench does not exit gracefully!", err_panic=False)

    stop_ufs(fs_proc)

    if not os.path.exists("./compressedLog"):
        logging.warning("No compressedLog found!")
        return

    os.rename("./compressedLog", f"{output_dir}/compressedLog")
    esc_path = str(PROJ_PATH).replace("/", "\/")
    sed_cmd = f"sed 's/{esc_path}\/cfs\/sched\///' | sed 's/{esc_path}\/cfs\///'"
    run_cmd(
        f"{PROJ_PATH}/cfs/lib/Nanolog/runtime/decompressor decompress "
        f"{output_dir}/compressedLog | {sed_cmd} > {output_dir}/ufs.nanolog",
        err_msg="Fail to decompress Nanolog!",
        err_panic=False,
        silent=True)
    run_cmd(
        f"grep 'Alloc Decision: ' < {output_dir}/ufs.nanolog "
        f"> {output_dir}/alloc.txt",
        err_panic=False,
        err_msg="No alloc found!",
        silent=True)
    run_cmd(
        f"grep 'Expect improvement ' < {output_dir}/ufs.nanolog "
        f"> {output_dir}/expect.txt",
        err_panic=False,
        err_msg="No expect found!",
        silent=True)
