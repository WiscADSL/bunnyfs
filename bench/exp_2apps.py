import os
import shutil
import time
from functools import partial

from ufs_ckpt import ufs_ckpt
from ufs_build import ufs_configure_then_build
from exp_2apps_spec import export_2apps_spec
from exp_utils import get_subdir_names, prepare_output_dir
from parse_var import parse_var
from plot_static import parse_and_plot_static
from plot_var import plot_var
from utils import run_bench
from ufs_run import get_ufs_cmd
from spec import OffsetType

# the SSD we are using has some issue if running too long; wait a while before
# each run
exp_sleep_sec = 1

# wrapper on top of get_ufs_cmd with default arguments
get_ufs_cmd_2apps = partial(
    get_ufs_cmd,
    num_apps=2,
    num_workers=4,
    total_cache_mb=1024,
    total_bandwidth_mbps=2048,
    core_ids=[33, 34, 35, 36]
)


def plot_2apps_exp_ws(static_name, vary_name, results_dir, show_expected=False):
    working_set_list = get_subdir_names(results_dir)
    for ws in working_set_list:
        parse_and_plot_static(results_dir / ws, skip_if_exists=True)
    df, expected_df = parse_var(results_dir, "working_set", working_set_list,
                                [static_name, vary_name])

    plot_var(df=df,
             expected_df=expected_df if show_expected else None,
             xlabel=f"Working set size\nof {vary_name} (GB)",
             results_dir=results_dir,
             vary_name=vary_name)


def plot_2apps_exp_theta(static_name, vary_name, results_dir, show_expected=False):
    zipf_theta_list = get_subdir_names(results_dir)
    for theta in zipf_theta_list:
        parse_and_plot_static(results_dir / theta, skip_if_exists=True)
    df, expected_df = parse_var(results_dir, "zipf_theta", zipf_theta_list,
                                [static_name, vary_name])

    plot_var(df=df,
             expected_df=expected_df if show_expected else None,
             xlabel=f"Zipfian theta\nof {vary_name}",
             results_dir=results_dir,
             vary_name=vary_name)


def plot_2apps_exp_wr(static_name, vary_name, results_dir, show_expected=False):
    working_set_list = get_subdir_names(results_dir)
    for ws in working_set_list:
        parse_and_plot_static(results_dir / ws, skip_if_exists=True)
    df, expected_df = parse_var(results_dir, "write_ratio", working_set_list,
                                [static_name, vary_name])

    plot_var(df=df,
             expected_df=expected_df if show_expected else None,
             xlabel=f"Write ratio\nof {vary_name}",
             results_dir=results_dir,
             vary_name=vary_name)


def run_2apps_exp_ws(output_dir, static_off, static_ws, vary_off, vary_ws_list,
                     static_name, vary_name, overwrite=False, var_read_ratio=1.0):
    ufs_configure_then_build(sched=True, leveldb=False,
                             fine_grained=False, high_freq=False)

    for ws in vary_ws_list:
        spec_path = export_2apps_spec(exp_name="2apps",
                                      off1=static_off,
                                      ws1=static_ws,
                                      off2=vary_off,
                                      ws2=ws,
                                      name1=static_name,
                                      name2=vary_name,
                                      read_ratio2=var_read_ratio)
        time.sleep(exp_sleep_sec)
        exp_dir = output_dir / f"{ws}"
        if os.path.exists(exp_dir):
            if overwrite:
                shutil.rmtree(exp_dir)
            else:
                raise ValueError("Experiment directory already exists!")
        if var_read_ratio != 1.0:
            ufs_ckpt()
        run_bench(spec_path, exp_dir, get_ufs_cmd_2apps())


def run_2apps_exp_theta(output_dir, static_off, static_ws, vary_ws,
                        vary_theta_list, static_name, vary_name, overwrite=False):
    ufs_configure_then_build(sched=True, leveldb=False,
                             fine_grained=False, high_freq=False)

    for theta in vary_theta_list:
        spec_path = export_2apps_spec(exp_name="2apps",
                                      off1=static_off,
                                      ws1=static_ws,
                                      off2=OffsetType.ZIPF,
                                      ws2=vary_ws,
                                      zipf_theta2=theta,
                                      name1=static_name,
                                      name2=vary_name)
        time.sleep(exp_sleep_sec)
        exp_dir = output_dir / f"{theta}"
        if os.path.exists(exp_dir):
            if overwrite:
                shutil.rmtree(exp_dir)
            else:
                raise ValueError("Experiment directory already exists!")
        run_bench(spec_path, exp_dir, get_ufs_cmd_2apps())


def run_2apps_exp_wr(output_dir, static_off, static_ws, vary_off, vary_ws,
                     vary_wr_list, static_name, vary_name, overwrite=False):
    ufs_configure_then_build(sched=True, leveldb=False,
                             fine_grained=False, high_freq=False)

    for wr in vary_wr_list:
        spec_path = export_2apps_spec(exp_name="2apps",
                                      off1=static_off,
                                      ws1=static_ws,
                                      off2=vary_off,
                                      ws2=vary_ws,
                                      name1=static_name,
                                      name2=vary_name,
                                      read_ratio2=1 - wr)
        time.sleep(exp_sleep_sec)
        exp_dir = output_dir / f"{wr}"
        if os.path.exists(exp_dir):
            if overwrite:
                shutil.rmtree(exp_dir)
            else:
                raise ValueError("Experiment directory already exists!")
        ufs_ckpt()
        run_bench(spec_path, exp_dir, get_ufs_cmd_2apps())
