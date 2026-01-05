#!/usr/bin/env python3

from ufs_build import get_ufs_build_dir
from utils import run_cmd


def ufs_ckpt() -> None:
    run_cmd(
        f'sudo -E {get_ufs_build_dir()}/test/fsproc/fsProcOfflineCheckpointer',
        ok_msg="DONE: uFS checkpointing",
        err_msg="Fail to checkpoint for uFS!"
    )


if __name__ == '__main__':
    ufs_ckpt()
