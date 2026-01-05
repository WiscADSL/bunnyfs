#!/usr/bin/env python3

import argparse
from typing import Tuple, Optional

from ufs_build import ufs_configure_then_build
from exp_utils import get_output_dir, prepare_output_dir
from plot_32apps import *
from plot_static import parse_and_plot_static
from spec import *
from spec_app import export_spec
from ufs_run import get_ufs_cmd
from utils import run_bench


def get_ufs_cmd_32apps(
    *,
    num_workers: int = 4,
    total_cache_mb: int = 2048,
    total_bandwidth_mbps: int = 2048,
    core_ids: List[int] = [33, 34, 35, 36],
    **kwargs
):
    """wrapper on top of get_ufs_cmd with default arguments"""
    return get_ufs_cmd(num_workers=num_workers,
                       num_apps=32,
                       total_cache_mb=total_cache_mb,
                       total_bandwidth_mbps=total_bandwidth_mbps,
                       core_ids=core_ids,
                       **kwargs)


def export_32apps_spec(
    exp_name,
    # each app -> (offset_type, working_set_gb, zipf_theta, name)
    app_configs: List[Tuple[OffsetType, float, float,
                            Optional[float], Optional[str]]],
    *,
    total_num_workers=4,
    num_threads_per_app=1,
    num_files_per_app=4,
    qdepth=8,
):
    return export_spec(exp_name=exp_name,
                       app_configs=app_configs,
                       total_num_workers=total_num_workers,
                       num_threads_per_app=num_threads_per_app,
                       num_files_per_app=num_files_per_app,
                       qdepth=qdepth,
                       is_symm=True,
                       use_affinity=False)


def run_exp_32apps(spec_path: Path):
    ufs_configure_then_build(sched=True, leveldb=False,
                             fine_grained=True, high_freq=False)

    output_dir_hare = prepare_output_dir("exp_32apps_hare")
    output_dir_drf = prepare_output_dir("exp_32apps_drf")
    output_dir_drfnp = prepare_output_dir("exp_32apps_drfnp")

    run_bench(spec_path, output_dir_hare, ufs_cmd=get_ufs_cmd_32apps())
    run_bench(spec_path, output_dir_drf,
              ufs_cmd=get_ufs_cmd_32apps(disable_harvest=True))
    run_bench(spec_path, output_dir_drfnp,
              ufs_cmd=get_ufs_cmd_32apps(disable_harvest=True,
                                         disable_cache_partition=True))


def plot_exp_32apps():
    output_dir_hare = get_output_dir("exp_32apps_hare")
    output_dir_drf = get_output_dir("exp_32apps_drf")
    output_dir_drfnp = get_output_dir("exp_32apps_drfnp")

    df_hare = parse_and_plot_static(output_dir_hare)
    df_drf = parse_and_plot_static(output_dir_drf)
    df_drfnp = parse_and_plot_static(output_dir_drfnp)

    plot_32apps_detailed(df_hare, df_drf, df_drfnp, output_dir_hare)
    plot_32apps_improve_cdf(df_hare, df_drf, df_drfnp, output_dir_hare)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--plot",
                        help="Only plot the data",
                        action="store_true")
    args = parser.parse_args()

    app_configs = [
        (offset_type, working_set_gb, read_ratio, zipf_theta, f"Tenant-{aid}")
        for (aid, (offset_type, working_set_gb, read_ratio, zipf_theta, _)) in enumerate(
            # (offset_type, working_set_gb, read_ratio, zipf_theta, name)
            (offset_type, working_set_gb, 1.0, zipf_theta, None)
            for (offset_type, zipf_theta) in [
                (OffsetType.ZIPF, 0.99),
                (OffsetType.ZIPF, 0.5),
                (OffsetType.UNIF, None),
                (OffsetType.SEQ, None)
            ]
            for working_set_gb in [(i + 1) * 1 / 16 for i in range(8)]
        )
    ]
    spec_path = export_32apps_spec(
        exp_name="exp_32apps", app_configs=app_configs)

    if not args.plot:
        run_exp_32apps(spec_path)

    plot_exp_32apps()


if __name__ == "__main__":
    main()
