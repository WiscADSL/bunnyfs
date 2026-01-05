#!/usr/bin/env python3

from typing import Union
from pathlib import Path
import pandas as pd
from matplotlib import pyplot as plt

from plot_util import export_fig
from parse_single import parse_single
from plot_single import plot_single
from parse_static import load_static_data


def plot_static(df_detailed: pd.DataFrame, df_summary: pd.DataFrame, path: Path):
    """augmented version of plot_single: report resource/tp in text"""
    fig = plot_single(df_detailed)

    columns = {
        "aid": "ID",
        "app": "Name",
        "tp_base": "Base",
        "tp_sched": "Sched",
        "improve": "I",
        "hit_rate": "Hit%",
        "cache_gb": "Cache",
        "bw_gbps": "BW",
        "cpu_cnt": "CPU",
    }
    title = df_summary[columns.keys()].rename(
        columns=columns).round(3).to_string(index=False)

    title_y = 1
    num_apps = len(df_detailed.groupby("app"))
    if num_apps <= 8:
        title_y += num_apps * 0.03
    elif len(df_detailed.groupby("app")) > 4:
        fig.set_size_inches(6.4, 9.6)
        title_y = 1.6
    fig.suptitle(title, fontfamily="monospace", y=title_y)

    export_fig(fig, path, "results")
    plt.close(fig)


def parse_and_plot_static(path: Union[str, Path], skip_if_exists=False):
    if isinstance(path, str):
        path = Path(path)
    fig_path = path / "results.png"
    if skip_if_exists and fig_path.exists():
        print(f"Skip plotting {fig_path} as it already exists")
        return
    df_detailed = parse_single(path)
    df_summary = load_static_data(path)
    plot_static(df_detailed, df_summary, path)
    return df_summary
