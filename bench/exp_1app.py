#!/usr/bin/env python3
from ufs_build import ufs_configure_then_build
from ufs_ckpt import ufs_ckpt
from exp_utils import prepare_output_dir
from spec import *
from spec_app import ExpConfig, WorkloadConfigPerApp
from ufs_run import get_ufs_cmd
from utils import run_bench


def export_1app_spec():
    exp_config = ExpConfig(
        num_workers=4,
        num_apps=1,
        num_threads_per_app=4,
        num_files_per_app=32,
        use_affinity=True,
        is_symm=True,
    )

    app = exp_config.get_app(0, "app0", WorkloadConfigPerApp(
        offset_type=OffsetType.UNIF,
        zipf_theta=None,
        working_set_gb=2,
        read_ratio=0.0,
        qdepth=8,
        duration_sec=20,
        count=16384,
    ))

    return exp_config.get_exp([app]).export_with_name("1app")


def main():
    output_dir = prepare_output_dir("1app")
    spec_path = export_1app_spec()
    ufs_configure_then_build(sched=True, leveldb=False,
                             fine_grained=False, high_freq=False)
    ufs_cmd = get_ufs_cmd(
        num_workers=4,
        num_apps=1,
        total_cache_mb=512,
        total_bandwidth_mbps=1024,
        core_ids=[17, 18, 19, 20]
    )
    ufs_ckpt()
    run_bench(spec_path, output_dir, ufs_cmd=ufs_cmd)


if __name__ == "__main__":
    main()