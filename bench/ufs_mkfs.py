#!/usr/bin/env python3

from ufs_build import get_ufs_build_dir, ufs_configure_then_build
from ufs_cleanup import ufs_cleanup
from utils import run_cmd


def ufs_mkfs() -> None:
    ufs_configure_then_build(sched=False, leveldb=False,
                             fine_grained=False, high_freq=False)

    ufs_cleanup()
    path = get_ufs_build_dir() / "test" / "fsproc" / "testRWFsUtil"
    run_cmd(
        f'sudo -E {path} mkfs',
        ok_msg="DONE: uFS mkfs",
        err_msg="Fail to mkfs for uFS!",
        silent=True,
    )


if __name__ == '__main__':
    ufs_mkfs()
