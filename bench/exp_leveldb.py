#!/usr/bin/env python3
import argparse
from enum import Enum
from typing import List

from ufs_ckpt import ufs_ckpt
from exp_leveldb_spec import gen_leveldb_spec
from exp_leveldb_prep import main as prep_leveldb
from exp_utils import prepare_output_dir
from plot_static import parse_and_plot_static
from ufs_build import ufs_configure_then_build
from ufs_run import get_ufs_cmd
from utils import run_bench


class Policy(Enum):
    DRFEP = "drf"
    DRFNP = "drfnp"
    HARE = "hare"

    @property
    def disable_harvest(self):
        return self != Policy.HARE

    @property
    def disable_cache_partition(self):
        return self == Policy.DRFNP

    @property
    def color(self):
        return {
            Policy.DRFEP: "0.5",
            Policy.DRFNP: "0.75",
            Policy.HARE: "0",
        }[self]

    @property
    def text_color(self):
        return {
            Policy.DRFEP: "0",
            Policy.DRFNP: "0",
            Policy.HARE: "1",
        }[self]

    @property
    def plot_name(self):
        return {
            Policy.DRFEP: "DRF",
            Policy.DRFNP: "DRF+NonPartCache",
            Policy.HARE: "HARE",
        }[self]


def main(policies: List[Policy]):
    spec = gen_leveldb_spec()
    spec_path = spec.export_with_name("leveldb")

    for cache_size_gb in [1, 2]:
        results_dir = prepare_output_dir(f"leveldb-{cache_size_gb}GB")
        for policy in policies:
            prep_leveldb(skip_mkfs=False)

            ufs_configure_then_build(sched=True, leveldb=True,
                                     fine_grained=False, high_freq=False)

            ufs_cmd = get_ufs_cmd(
                num_apps=len(spec.apps),
                num_workers=4,
                core_ids=list(range(33, 33 + 4)),  # we have 32 application threads
                disable_harvest=policy.disable_harvest,
                disable_cache_partition=policy.disable_cache_partition,
                total_cache_mb=cache_size_gb * 1024,
                total_bandwidth_mbps=2048,
            )

            ufs_ckpt()
            output_dir = results_dir / policy.value
            run_bench(spec_path, output_dir, ufs_cmd)
            parse_and_plot_static(output_dir)

        from exp_leveldb_plot import plot_results
        plot_results(results_dir)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--policies",
        choices=[p.value for p in Policy],
        default=[p.value for p in Policy],
        nargs="+",
    )
    args = parser.parse_args()
    main(policies=[Policy(p) for p in args.policies])
