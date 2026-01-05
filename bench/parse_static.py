# parse all the data into one table
import json
import logging
import re
from pathlib import Path
from typing import List, Tuple, Dict

import pandas as pd


def compute_throughput(df: pd.DataFrame):
    # check every epoch has output from every thread
    # we only run experiments with 16 or 32 threads, so we expect to see as many
    # threads' data
    num_thread = df.groupby("epoch", as_index=False).count().min()["thread"]
    if num_thread not in {8, 16, 32}:
        logging.warning(f"Missing epoch/thread data: only {num_thread} threads")
    df = df[["app", "epoch", "throughput"]]
    df_agg = df.groupby(["app", "epoch"], as_index=False).sum()
    # the system starts to do allocation from 15s (and then every 30s)
    tp_base = df_agg[
        (df_agg["epoch"] >= 9) & (df_agg["epoch"] <= 13)
    ].groupby("app", as_index=False).mean()
    tp_sched = df_agg[
        (df_agg["epoch"] >= 25) & (df_agg["epoch"] <= 29)
    ].groupby("app", as_index=False).mean()
    return tp_base, tp_sched


def load_throughput(data_path) -> Tuple[List, List]:
    df = pd.read_csv(data_path)
    tp_base_list, tp_sched_list = compute_throughput(df)
    tp_base_list = tp_base_list[["app", "throughput"]]
    tp_sched_list = tp_sched_list[["app", "throughput"]]
    return tp_base_list.to_numpy(), tp_sched_list.to_numpy()


def convert_df(df: pd.DataFrame) -> pd.DataFrame:
    df["tp_base_raw"] = df["tp_base"]
    df["tp_sched_raw"] = df["tp_sched"]
    df["tp_sched"] /= 1024
    df["tp_base"] /= 1024
    df["tp_norm"] = df["tp_sched"] / df["tp_base"]
    df["improve"] = df["tp_norm"] - 1
    df["cache_gb"] = df["cache"] * 4096 / 1024 / 1024 / 1024
    df["bw_gbps"] = df["bw"] * 4096 / 1024 / 1024 / 1024
    df["cache_mb"] = df["cache"] * 4096 / 1024 / 1024
    df["bw_mbps"] = df["bw"] * 4096 / 1024 / 1024
    df["cpu_cnt"] = df["cpu"] / 1898971136  # cycles to cpu count
    return df


def load_static_data(result_dir: str, apps_names=None) -> pd.DataFrame:
    """load all data and merge into a dataframe"""
    app_name_to_aid = {}
    if apps_names is None:
        spec = Path(result_dir) / "spec.json"
        spec_json = json.load(spec.open())
        apps_names = [app["name"] for app in spec_json["apps"]]
        app_name_to_aid = {app["name"]: app["aid"]
                           for app in spec_json["apps"]}

    tp_base_list, tp_sched_list = load_throughput(f"{result_dir}/results.csv")
    app_alloc_map = load_alloc(f"{result_dir}/alloc.txt")

    app_map = {
        app: {"aid": app_name_to_aid.get(app, -1), "app": app, "tp_base": tp}
        for app, tp in tp_base_list
    }
    for app, tp in tp_sched_list:
        app_map[app]["tp_sched"] = tp
    merge_alloc(apps_names, app_map, app_alloc_map)
    df = pd.DataFrame(app_map.values())
    df = convert_df(df)
    df = df.sort_values(by="aid")
    df.to_csv(f"{result_dir}/data.csv", index=False)
    df.to_string(f"{result_dir}/data.txt", index=False)
    print(f"Saving app map to {result_dir}/data.csv")
    print(df.to_string(index=False))
    return df


def load_alloc(data_path) -> Dict:
    if not Path(data_path).exists():
        logging.warning(f"Missing alloc file: {data_path}")
        raise FileNotFoundError(data_path)
    dec_re = re.compile(
        r".+Alloc Decision: App-(?P<app_id>\d+): cache=(?P<cache>\d+), "
        r"bw=(?P<bw>\d+), cpu=(?P<cpu>\d+), "
        r"hit_rate=(?P<hit_rate>\d+\.\d+); .+")
    app_alloc_map = {}
    with open(data_path) as f:
        for line in f:
            m = re.match(dec_re, line)
            if m is None:
                logging.warning(f"Fail to match in alloc.txt: {line}")
                continue
            m_dict = m.groupdict()
            if m_dict["app_id"] in app_alloc_map:
                logging.warning(
                    f"App-{m_dict['app_id']} allocation already exist; skip...")
                continue
            app_alloc_map[m_dict["app_id"]] = {
                "cache": int(m_dict["cache"]),
                "bw": int(m_dict["bw"]),
                "cpu": int(m_dict["cpu"]),
                "hit_rate": float(m_dict["hit_rate"]),
            }
    return app_alloc_map


def load_expect(data_path) -> float:
    if not Path(data_path).exists():
        logging.warning(f"Missing expect file: {data_path}")
        raise FileNotFoundError(data_path)
    # for now, we only parse expected improvement; maybe more in the future
    exp_re = re.compile(r".+Expect improvement after CPU-distribution: "
                        r"(?P<expect_improv>\d+\.\d+)%")
    with open(data_path) as f:
        for line in f:
            m = re.match(exp_re, line)
            if m is not None:
                m_dict = m.groupdict()
                return m_dict["expect_improv"]  # as a string
        assert False, f"Unexpected line in {data_path}"


def merge_alloc(apps_names, app_map, app_alloc_map) -> None:
    # match aid to app_name; update to app_map
    for aid, app_name in enumerate(apps_names):
        assert app_name in app_map
        app_map[app_name].update(app_alloc_map[f"{aid}"])
