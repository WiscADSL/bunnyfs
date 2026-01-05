#!/usr/bin/env python3

"""
This script prepares the LevelDB database for the experiment.
"""
import argparse

from exp_utils import prepare_output_dir
from spec import Database, Exp, Prep
from ufs_build import ufs_configure_then_build
from ufs_ckpt import ufs_ckpt
from ufs_mkfs import ufs_mkfs
from ufs_run import get_ufs_cmd
from utils import run_bench

VALUE_SIZE = 100


def get_prep_dbs(num_dbs=32, size_per_db=520 * 1024 * 1024):
    num_keys = size_per_db // VALUE_SIZE
    return [
        Database(
            path=f"db-{i}",
            value_size=VALUE_SIZE,
            num_keys=num_keys,
        )
        for i in range(num_dbs)
    ]


def prep_leveldb(dbs):
    exp = Exp(
        num_workers=1,
        apps=[],  # No apps, only prepare the DBs.
        prep=Prep(databases=dbs),
    )
    spec_path = exp.export_with_name("leveldb_prep")
    output_dir = prepare_output_dir("leveldb_prep")
    ufs_cmd = get_ufs_cmd(
        num_workers=1,
        num_apps=1,
        total_cache_mb=8000,
        total_bandwidth_mbps=4000,
        core_ids=[17]
    )
    run_bench(
        spec_fname=spec_path,
        output_dir=output_dir,
        ufs_cmd=ufs_cmd,
        prep=True,
        timeout=10 * 60,  # 10 minutes
    )


def main(skip_mkfs):
    if not skip_mkfs:
        ufs_mkfs()
    # Note: it is important that this comes after ufs_mkfs() since
    # ufs_mkfs() rebuilds the ufs binary with another configuration.
    ufs_configure_then_build(sched=False, leveldb=True,
                             fine_grained=False, high_freq=False)

    dbs = get_prep_dbs()
    # break dbs into chunks of size 4
    for i in range(0, len(dbs), 4):
        prep_leveldb(dbs[i:i + 4])
        ufs_ckpt()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--skip-mkfs", action="store_true")
    args = parser.parse_args()
    main(args.skip_mkfs)
