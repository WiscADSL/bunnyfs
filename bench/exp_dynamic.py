#!/usr/bin/env python3
import argparse
from pathlib import Path
from typing import Optional, Union, List

from ufs_build import ufs_configure_then_build
from ufs_run import get_ufs_cmd
from exp_utils import prepare_output_dir, get_output_dir
from spec import OffsetType, Prep, Exp
from utils import run_bench
from spec_app import ExpConfig, WorkloadConfigPerApp
from plot_single import parse_and_plot_single
from plot_dynamic import plot_dynamic_by_policy, plot_dynamic_by_tenant, make_dynamic_legend_tenant, make_dynamic_legend_policy


def get_ufs_cmd_dynamic(
    *,
    num_workers: int = 4,
    total_cache_mb: int = 2048,
    total_bandwidth_mbps: int = 2048,
    core_ids: List[int] = [17, 18, 19, 20],
    **kwargs
):
    return get_ufs_cmd(num_workers=num_workers,
                       num_apps=4,
                       total_cache_mb=total_cache_mb,
                       total_bandwidth_mbps=total_bandwidth_mbps,
                       core_ids=core_ids,
                       **kwargs)


def get_workload_config_pairs(
        offset_type: OffsetType,
        changing_ts: int,
        # the following parameters can be either a single value or a list of values of length 2
        working_set_gb: Union[float, List[float]],
        zipf_theta: Union[Optional[float], List[float]] = None,
        total_duration_sec: int = 70,
):
    is_ws_varied = isinstance(working_set_gb, list) and len(working_set_gb) == 2
    is_zt_varied = isinstance(zipf_theta, list) and len(zipf_theta) == 2
    assert is_ws_varied ^ is_zt_varied, (
        "Exactly one of working_set_gb and zipf_theta should be a list of length 2"
    )

    if not isinstance(working_set_gb, list):
        working_set_gb = [working_set_gb] * 2
    if not isinstance(zipf_theta, list):
        zipf_theta = [zipf_theta] * 2
    result = [
        WorkloadConfigPerApp(
            offset_type=offset_type,
            working_set_gb=ws,
            duration_sec=ds,
            zipf_theta=zt,
            count=16384,
            read_ratio=1.0,
            qdepth=8,
        )
        for (ws, zt, ds) in zip(
            working_set_gb,
            zipf_theta,
            [changing_ts, total_duration_sec - changing_ts]
        )
    ]
    return result


def export_dynamic_spec():
    exp_config = ExpConfig(
        num_workers=4,
        num_apps=4,
        num_threads_per_app=4,
        num_files_per_app=32,
        use_affinity=True,
        is_symm=True,
    )

    app1_wl = get_workload_config_pairs(
        offset_type=OffsetType.UNIF,
        working_set_gb=[2, 1],
        changing_ts=25,
    )

    app2_wl = get_workload_config_pairs(
        offset_type=OffsetType.ZIPF,
        working_set_gb=[2, 1],
        changing_ts=35,
        zipf_theta=0.99,
    )

    app3_wl = get_workload_config_pairs(
        offset_type=OffsetType.ZIPF,
        working_set_gb=1.5,
        changing_ts=45,
        zipf_theta=[0.5, 0.99],
    )

    app4_wl = get_workload_config_pairs(
        offset_type=OffsetType.SEQ,
        working_set_gb=[2, 0.499],
        changing_ts=55,
    )

    apps = [
        exp_config.get_app(aid, name, wls)
        for (aid, (name, wls)) in enumerate([
            ("Tenant-U", app1_wl),
            ("Tenant-Z1", app2_wl),
            ("Tenant-Z2", app3_wl),
            ("Tenant-S", app4_wl),
        ])
    ]

    exp = Exp(
        num_workers=exp_config.num_workers,
        apps=apps,
        prep=Prep(files=exp_config.files),
    )

    return exp.export_with_name("exp_dynamic")


def run_exp_dynamic(spec_path: Path):
    ufs_configure_then_build(sched=True, leveldb=False,
                             fine_grained=False, high_freq=True)

    output_dir_base = prepare_output_dir("exp_dynamic_base")
    output_dir_hare = prepare_output_dir("exp_dynamic_hare")
    output_dir_drf = prepare_output_dir("exp_dynamic_drf")
    output_dir_drfnp = prepare_output_dir("exp_dynamic_drfnp")

    run_bench(spec_path, output_dir_base,
              ufs_cmd=get_ufs_cmd_dynamic(disable_alloc=True))
    run_bench(spec_path, output_dir_hare, ufs_cmd=get_ufs_cmd_dynamic())
    run_bench(spec_path, output_dir_drf,
              ufs_cmd=get_ufs_cmd_dynamic(disable_harvest=True))
    run_bench(spec_path, output_dir_drfnp,
              ufs_cmd=get_ufs_cmd_dynamic(disable_harvest=True,
                                          disable_cache_partition=True))


def plot_exp_dynamic():
    output_dir_base = get_output_dir("exp_dynamic_base")
    output_dir_hare = get_output_dir("exp_dynamic_hare")
    output_dir_drf = get_output_dir("exp_dynamic_drf")
    output_dir_drfnp = get_output_dir("exp_dynamic_drfnp")

    df_base = parse_and_plot_single(output_dir_base)
    df_hare = parse_and_plot_single(output_dir_hare)
    df_drf = parse_and_plot_single(output_dir_drf)
    df_drfnp = parse_and_plot_single(output_dir_drfnp)

    # # plot normalized throughput
    # plot_dynamic_by_policy(df=df_base, df_base=df_base, policy_name="base",
    #                        results_dir=output_dir_base)  # should be mostly flat
    # plot_dynamic_by_policy(df=df_hare, df_base=df_base, policy_name="hare",
    #                        results_dir=output_dir_hare)
    # plot_dynamic_by_policy(df=df_drf, df_base=df_base, policy_name="drf",
    #                        results_dir=output_dir_drf)
    # plot_dynamic_by_policy(df=df_drfnp, df_base=df_base,
    #                        policy_name="drfnp",  results_dir=output_dir_drfnp)

    # # plot absolute throughput
    # plot_dynamic_by_policy(df=df_base, df_base=None,
    #                        policy_name="base", results_dir=output_dir_base)
    # plot_dynamic_by_policy(df=df_hare, df_base=None,
    #                        policy_name="hare", results_dir=output_dir_hare)
    # plot_dynamic_by_policy(df=df_drf, df_base=None,
    #                        policy_name="drf", results_dir=output_dir_drf)
    # plot_dynamic_by_policy(df=df_drfnp, df_base=None,
    #                        policy_name="drfnp", results_dir=output_dir_drfnp)

    # plot by tenant
    plot_dynamic_by_tenant(df_base=df_base, df_hare=df_hare,
                           df_drf=df_drf, df_drfnp=df_drfnp,
                           results_dir=output_dir_hare)

    # make_dynamic_legend_tenant(output_dir_hare / "dynamic_legend_tenant")
    make_dynamic_legend_policy(output_dir_hare / "dynamic_legend_policy")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--plot",
                        help="Only plot the data",
                        action="store_true")
    args = parser.parse_args()

    spec_path = export_dynamic_spec()

    if not args.plot:
        run_exp_dynamic(spec_path)
    plot_exp_dynamic()


if __name__ == "__main__":
    main()
