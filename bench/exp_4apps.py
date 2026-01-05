#!/usr/bin/env python3

import argparse
from typing import Tuple, Optional

from exp_utils import get_output_dir, prepare_output_dir
from plot_4apps import *
from plot_static import parse_and_plot_static
from spec import *
from spec_app import export_spec
from ufs_run import get_ufs_cmd
from ufs_build import ufs_configure_then_build
from utils import run_bench


def get_ufs_cmd_4apps(
    *,
    num_workers: int = 4,
    total_cache_mb: int = 2048,
    total_bandwidth_mbps: int = 2048,
    core_ids: List[int] = [33, 34, 35, 36],
    **kwargs
):
    """wrapper on top of get_ufs_cmd with default arguments"""
    return get_ufs_cmd(num_workers=num_workers,
                       num_apps=4,
                       total_cache_mb=total_cache_mb,
                       total_bandwidth_mbps=total_bandwidth_mbps,
                       core_ids=core_ids,
                       **kwargs)


def export_4apps_spec(
    exp_name,
    # each app -> (offset_type, working_set_gb, zipf_theta, name)
    app_configs: List[Tuple[OffsetType, float, float,
                            Optional[float], Optional[str]]],
    *,
    total_num_workers=4,
    num_threads_per_app=4,
    num_files_per_app=32,
    qdepth=8,
):
    return export_spec(exp_name=exp_name,
                       app_configs=app_configs,
                       total_num_workers=total_num_workers,
                       num_threads_per_app=num_threads_per_app,
                       num_files_per_app=num_files_per_app,
                       qdepth=qdepth,
                       use_affinity=True)


def run_exp_4apps(spec_path: str):
    ufs_configure_then_build(sched=True, leveldb=False,
                             fine_grained=False, high_freq=False)
    output_dir_hare = prepare_output_dir("exp_4apps_hare")
    output_dir_drf = prepare_output_dir("exp_4apps_drf")

    run_bench(spec_path, output_dir_hare, ufs_cmd=get_ufs_cmd_4apps())
    run_bench(spec_path, output_dir_drf,
              ufs_cmd=get_ufs_cmd_4apps(disable_harvest=True))


def plot_exp_4apps(app_names: List[str]):
    output_dir_hare = get_output_dir("exp_4apps_hare")
    output_dir_drf = get_output_dir("exp_4apps_drf")

    df_hare = parse_and_plot_static(output_dir_hare)
    df_drf = parse_and_plot_static(output_dir_drf)

    plot_4apps(df_hare, df_drf, app_names, output_dir_hare)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--plot",
                        help="Only plot the data",
                        action="store_true")
    args = parser.parse_args()

    app_configs = [  # (offset_type, working_set_gb, zipf_theta, name)
        (OffsetType.ZIPF, 2, 1.0, 0.99, "Tenant X"),
        (OffsetType.UNIF, 2, 1.0, None, "Tenant U"),
        (OffsetType.ZIPF, 2, 1.0, 0.5, "Tenant Z"),
        (OffsetType.SEQ, 0.1, 1.0, None, "Tenant S")]
    spec_path = export_4apps_spec(
        exp_name="exp_4apps", app_configs=app_configs)

    if not args.plot:
        run_exp_4apps(spec_path)

    # this determines the plot order
    plot_exp_4apps(app_names=[
        "Tenant X", "Tenant U", "Tenant Z", "Tenant S"])


if __name__ == "__main__":
    main()
