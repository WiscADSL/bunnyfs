#!/usr/bin/env python3

"""
This script prepares the files for the experiment.
"""
import argparse

from ufs_build import ufs_configure_then_build
from exp_utils import prepare_output_dir
from spec import *
from ufs_ckpt import ufs_ckpt
from ufs_mkfs import ufs_mkfs
from ufs_run import get_ufs_cmd
from utils import run_bench


def get_prep_files(num_files=128, size_per_file=128 * 1024 * 1024):
    # Note: uFS is not good at creating many files larger than 128MB.
    return [
        PrepFile(path=f"/f_{i}", size=size_per_file)
        for i in range(num_files)
    ]


def export_prep_spec():
    files = get_prep_files()
    exp = Exp(
        num_workers=1,
        apps=[],
        prep=Prep(files=files)
    )
    return exp.export_with_name("prep")


def prep_files():
    spec_path = export_prep_spec()
    output_dir = prepare_output_dir("prep_files")
    run_bench(
        spec_fname=spec_path,
        output_dir=output_dir,
        ufs_cmd=get_ufs_cmd(num_workers=1, num_apps=1,
                            total_cache_mb=1000, total_bandwidth_mbps=2500,
                            core_ids=[17]),
        prep=True,
    )


def main(skip_mkfs):
    ufs_configure_then_build(sched=False, leveldb=False,
                             fine_grained=False, high_freq=False)
    if not skip_mkfs:
        ufs_mkfs()
    prep_files()
    ufs_ckpt()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--skip-mkfs", action="store_true")
    args = parser.parse_args()
    main(args.skip_mkfs)
