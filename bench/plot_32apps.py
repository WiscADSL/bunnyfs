from pathlib import Path

from typing import List
import matplotlib
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np
import pandas as pd

from plot_util import *

matplotlib.rcParams['pdf.fonttype'] = 42
matplotlib.rcParams['ps.fonttype'] = 42

tp_ticks = [0, 2, 4, 6, 8]
cache_ticks = [0, 0.2, 0.4, 0.6, 0.8, 1]
bw_ticks = [0, 0.25, 0.5, 0.75, 1]
cpu_ticks = [0, 0.5, 1, 1.5, 2]

textcolor_map = {
    "base": "1",
    "drf": "0",
    "drfnp": "0",
    "hare": "1",
}

color_map = {
    "base": "0.1",
    "drf": "0.5",
    "drfnp": "0.75",
    "hare": "0",
}

linestyle_map = {
    "base": ":",
    "drf": "--",
    "drfnp": "-.",
    "hare": "-",
}

label_map = {
    "base": "Base",
    "drf": "DRF",
    "drfnp": "NonPartCache+DRF",
    "hare": "HARE",
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


def plot_32apps_tp(ax, df_hare: pd.DataFrame, df_drf: pd.DataFrame,
                   df_drfnp: pd.DataFrame):
    # for each policy, plot the throughput of each app in a bar
    x_list = np.array(range(32), dtype=float)
    bar_width = 0.21
    for idx, (name, tp, tp_norm) in enumerate([
        ("base", df_hare["tp_base"], 100),
        ("drfnp", df_drfnp["tp_sched"],
         df_drfnp["tp_sched"] / df_hare["tp_base"] * 100),
        ("drf", df_drf["tp_sched"],
         df_drf["tp_sched"] / df_hare["tp_base"] * 100),
        ("hare", df_hare["tp_sched"],
         df_hare["tp_sched"] / df_hare["tp_base"] * 100),
    ]):
        rect = ax.bar(
            x_list + idx * bar_width,
            tp_norm,
            width=bar_width,
            color=color_map[name],
            label=label_map[name],
        )
        ax.bar_label(
            rect,
            labels=[f" {t * 1024:.0f}" for t in tp],
            fontsize=10,
            rotation=90,
            label_type='edge',
        )

    ax.set_xticks(x_list + bar_width, [str(i + 1) for i in range(32)])
    ax.set_xlim(-0.5, 32)
    ax.set_xlabel("Tenants")
    ax.tick_params(axis='y', rotation=90)
    ax.set_ylim(0, 300)
    ax.set_ylabel("Normalized\nThroughput (%)")
    ax.spines[['right', 'top']].set_visible(False)

    ax.legend(
        loc="upper center",
        ncol=4,
        frameon=False,
        bbox_to_anchor=(0.5, 1.2),
    )


def get_bar_start(col: List[float]):
    return [sum(col[:i]) for i in range(len(col))]


def plot_32apps_cache(ax, df_hare: pd.DataFrame, df_drf: pd.DataFrame,
                      df_drfnp: pd.DataFrame):
    bar_height = 0.4

    ax.invert_yaxis()

    for idx, (name, cache_mb) in enumerate([
        ("base", [64] * 32),
        ("drfnp", df_drfnp["cache_mb"]),
        ("drf", df_drf["cache_mb"]),
        ("hare", df_hare["cache_mb"]),
    ]):
        rects = ax.barh(idx * 0.5, width=cache_mb, left=get_bar_start(cache_mb),
                        height=bar_height, color=color_map[name])
        for r in rects:
            r.set_edgecolor(textcolor_map[name])
            r.set_linewidth(1)

        for i, rect in enumerate(rects):
            if rect.get_width() < 16:  # skip if no space
                continue
            ax.text(
                rect.get_x() + rect.get_width() / 2,
                rect.get_y(),
                f"{i + 1}",
                ha="center",
                va="bottom",
                color=textcolor_map[name],
            )

    ax.set_xticks(list(range(0, 2049, 512)))
    ax.set_xlim(0, 2048)
    ax.set_xlabel("Cache (MB)")
    ax.tick_params(axis='y', rotation=90)
    ax.set_ylim(-0.2, 1.65)
    ax.set_yticks([])
    ax.spines[['left', 'right', 'top']].set_visible(False)


def plot_32apps_bw(ax, df_hare: pd.DataFrame, df_drf: pd.DataFrame,
                   df_drfnp: pd.DataFrame):
    bar_height = 0.4

    ax.invert_yaxis()

    for idx, (name, bw_mbps) in enumerate([
        ("base", [64] * 32),
        ("drfnp", df_drfnp["bw_mbps"]),
        ("drf", df_drf["bw_mbps"]),
        ("hare", df_hare["bw_mbps"]),
    ]):
        rects = ax.barh(idx * 0.5, width=bw_mbps, left=get_bar_start(bw_mbps),
                        height=bar_height, color=color_map[name])
        for r in rects:
            r.set_edgecolor(textcolor_map[name])
            r.set_linewidth(1)

        for i, rect in enumerate(rects):
            if rect.get_width() < 16:  # skip if no space
                continue
            ax.text(
                rect.get_x() + rect.get_width() / 2,
                rect.get_y(),
                f"{i + 1}",
                ha="center",
                va="bottom",
                color=textcolor_map[name],
            )

    ax.set_xticks(list(range(0, 2049, 512)))
    ax.set_xlim(0, 2048)
    ax.set_xlabel("Bandwidth (MB/s)")
    ax.tick_params(axis='y', rotation=90)
    ax.set_ylim(-0.2, 1.65)
    ax.set_yticks([])
    ax.spines[['left', 'right', 'top']].set_visible(False)


def plot_32apps_cpu(ax, df_hare: pd.DataFrame, df_drf: pd.DataFrame, df_drfnp: pd.DataFrame):
    bar_height = 0.4

    ax.invert_yaxis()

    for idx, (name, cpu_cnt) in enumerate([
        ("base", [0.125] * 32),
        ("drfnp", df_drfnp["cpu_cnt"]),
        ("drf", df_drf["cpu_cnt"]),
        ("hare", df_hare["cpu_cnt"]),
    ]):
        rects = ax.barh(idx * 0.5, width=cpu_cnt, left=get_bar_start(cpu_cnt),
                        height=bar_height, color=color_map[name])
        for r in rects:
            r.set_edgecolor(textcolor_map[name])
            r.set_linewidth(1)

        for i, rect in enumerate(rects):
            if rect.get_width() < 0.03125:  # skip if no space
                continue
            ax.text(
                rect.get_x() + rect.get_width() / 2,
                rect.get_y(),
                f"{i + 1}",
                ha="center",
                va="bottom",
                color=textcolor_map[name],
            )

    ax.set_xticks(list(range(5)))
    ax.set_xlim(0, 4)
    ax.set_xlabel("Number of CPUs")
    ax.tick_params(axis='y', rotation=90)
    ax.set_ylim(-0.2, 1.65)
    ax.set_yticks([])
    ax.spines[['left', 'right', 'top']].set_visible(False)


def plot_32apps_detailed(df_hare: pd.DataFrame, df_drf: pd.DataFrame,
                         df_drfnp: pd.DataFrame, results_dir: Path):
    # make sure the baseline in df_hare and df_drf are the same
    assert np.allclose(df_hare["tp_base"], df_drf["tp_base"], rtol=0.05)

    fig = plt.figure(figsize=(16, 6))
    ax_tp = plt.subplot(5, 1, (1, 2))
    ax_cache = plt.subplot(5, 1, (3, 3))
    ax_bw = plt.subplot(5, 1, (4, 4))
    ax_cpu = plt.subplot(5, 1, (5, 5))

    plot_32apps_tp(ax=ax_tp, df_hare=df_hare, df_drf=df_drf, df_drfnp=df_drfnp)
    plot_32apps_cache(ax=ax_cache, df_hare=df_hare,
                      df_drf=df_drf, df_drfnp=df_drfnp)
    plot_32apps_bw(ax=ax_bw, df_hare=df_hare, df_drf=df_drf, df_drfnp=df_drfnp)
    plot_32apps_cpu(ax=ax_cpu, df_hare=df_hare,
                    df_drf=df_drf, df_drfnp=df_drfnp)

    fig.set_tight_layout({"pad": 0.05, "w_pad": 0.05, "h_pad": 0.2})
    export_fig(fig, dir=results_dir, name="result_32apps_detailed")
    plt.close(fig)


def plot_32apps_improve_cdf(df_hare: pd.DataFrame, df_drf: pd.DataFrame,
                            df_drfnp: pd.DataFrame, results_dir: Path):
    # make sure the baseline in df_hare and df_drf are the same
    assert np.allclose(df_hare["tp_base"], df_drf["tp_base"], rtol=0.05)
    fig = plt.figure(figsize=(SINGLE_COLUMN_WIDTH * 0.7, SINGLE_COLUMN_WIDTH * 0.3))
    ax_impr = plt.subplot(1, 1, 1)

    x, y = get_cdf(df_hare["tp_base"] / df_hare["tp_base"])
    ax_impr.plot(x, y, label=label_map["base"],
                 color=color_map["base"], linestyle=linestyle_map["base"])
    for name, df in [
        ("drf", df_drf),
        ("drfnp", df_drfnp),
        ("hare", df_hare),
    ]:
        x, y = get_cdf(df["tp_sched"] / df_hare["tp_base"])
        ax_impr.plot(x, y, label=label_map[name],
                     color=color_map[name], linestyle=linestyle_map[name],
                     linewidth=1)

    xticks = [0, 0.5, 1, 1.5, 2, 2.5]

    ax_impr.set_xticks(xticks,)
    ax_impr.set_xlim(xticks[0], xticks[-1])
    ax_impr.set_xlabel("Normalized Throughput")
    yticks = ["0", ".2", ".4", ".6", ".8", "1"]
    ax_impr.set_yticks([float(t) for t in yticks], yticks)
    ax_impr.set_ylim(0, 1)
    ax_impr.set_ylabel("CDF")

    # ax_impr.legend(
    #     loc="upper center",
    #     ncol=4,
    #     frameon=False,
    #     bbox_to_anchor=(0.45, 1.25),
    #     columnspacing=1.2,
    # )

    ax_impr.spines[['right', 'top']].set_visible(False)

    fig.set_tight_layout({"pad": 0.0, "w_pad": 0.0, "h_pad": 0.0})
    export_fig(fig, dir=results_dir, name="result_32apps_improve_cdf")
    plt.close(fig)
