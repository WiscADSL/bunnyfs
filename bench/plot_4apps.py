from pathlib import Path

from typing import List
import matplotlib
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np

from plot_util import *

matplotlib.rcParams['pdf.fonttype'] = 42
matplotlib.rcParams['ps.fonttype'] = 42


tp_ticks = [0, 2, 4, 6, 8]
cache_ticks = [0, 0.2, 0.4, 0.6, 0.8, 1]
bw_ticks = [0, 0.25, 0.5, 0.75, 1]
cpu_ticks = [0, 0.5, 1, 1.5, 2]

color_map = {
    "base": "#91bfdb",
    "sched": "#d7191c",
    "drf": "#fdae61",
}

label_map = {
    "base": "Baseline",
    "sched": "HARE",
    "drf": "DRF+Conserving",
}


def get_data(df, app_list, col):
    # extract a col from df, ordered by app_list
    data = []
    for app in app_list:
        d = df[(df["app"] == app)]
        if d.shape[0] != 1:
            raise ValueError(f"Unexpected data: app={app}, df={df}")
        data.append(d[col].to_list()[0])
    return np.array(data)


def make_legend_patch(labels, fname, height=0.15,
                      columnspacing=2, fontsize=10):
    pseudo_fig = plt.figure()
    ax = pseudo_fig.add_subplot(111)

    bars = [
        mpatches.Patch(color=color_map[l], label=label_map[l])
        for l in labels
    ]

    legend_fig = plt.figure()
    legend_fig.set_size_inches(5, height)
    legend_fig.legend(bars,
                      [label_map[l] for l in labels],
                      loc='center',
                      ncol=len(labels),
                      fontsize=fontsize,
                      frameon=False,
                      columnspacing=columnspacing,
                      labelspacing=0.4)
    legend_fig.set_tight_layout({"pad": 0, "w_pad": 0, "h_pad": 0})
    print(f"Legend saved to {fname}.jpg")
    legend_fig.savefig(f"{fname}.jpg")
    print(f"Legend saved to {fname}.pdf")
    legend_fig.savefig(f"{fname}.pdf")
    plt.close(legend_fig)


def plot_4apps(df_hare, df_drf, app_list, results_dir: Path):
    assert len(app_list) == 4

    # make sure that the apps in df_hare and df_drf are the same as app_list (in the same order)
    assert df_hare["app"].to_list() == app_list
    assert df_drf["app"].to_list() == app_list

    fig, ax_tp = plt.subplots(nrows=1, ncols=1)
    fig.set_size_inches(5, 1.8)
    ax_tp.spines[['right', 'top']].set_visible(False)
    # (ax_tp, ax_cache), (ax_bw, ax_cpu) = axes

    # make sure the baseline in df_hare and df_drf are the same
    assert np.allclose(df_hare["tp_base"], df_drf["tp_base"], rtol=0.05)

    fig, ax_tp = plt.subplots(figsize=(6, 2))

    # for each policy, plot the throughput of each app in a bar
    x_list = np.array(range(len(app_list)), dtype=float)
    bar_width = 0.3
    for idx, (name, tp, tp_norm) in enumerate([
        ("base", df_hare["tp_base"], 100),
        ("sched", df_hare["tp_sched"], df_hare["tp_norm"] * 100),
        ("drf", df_drf["tp_sched"], df_drf["tp_norm"] * 100),
    ]):
        rect = ax_tp.bar(
            x_list + idx * bar_width,
            tp_norm,
            width=bar_width,
            color=color_map[name],
            label=label_map[name],
        )
        ax_tp.bar_label(
            rect,
            labels=[f"{t:.1f}\nGB/s" for t in tp],
            fontsize=8.75
        )

    ax_tp.set_xticks(x_list + bar_width, ["X", "U", "Z", "S"])
    ax_tp.set_xlabel("Tenants")
    ax_tp.tick_params(axis='y', rotation=90)
    ax_tp.set_ylim(0, 200)
    ax_tp.set_ylabel("Normalized Throughput (%)")
    ax_tp.spines[['right', 'top']].set_visible(False)

    ax_tp.legend(
        loc="upper center",
        ncol=3,
        frameon=False,
        bbox_to_anchor=(0.5, 1.2),
    )

    export_fig(fig, dir=results_dir, name="result")
    plt.close(fig)
