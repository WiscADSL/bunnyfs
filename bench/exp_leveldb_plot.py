#!/usr/bin/env python3
import argparse
from enum import Enum
from pathlib import Path
from typing import List

import matplotlib.pyplot as plt
import numpy as np

from exp_utils import resolve_output_dir
from exp_leveldb import Policy
from exp_leveldb_spec import app_dicts
from parse_static import load_static_data
from plot_util import export_fig, save_legend, SINGLE_COLUMN_WIDTH


def plot_tp(ax, policy_to_df, app_names, plot_min=False):
    bar_width = 0.28
    df_hare = policy_to_df[Policy.HARE]
    if plot_min:
        xs = np.array(0, dtype=float)
    else:
        xs = np.array(range(len(df_hare)), dtype=float)
    for method_idx, (policy, df) in enumerate(policy_to_df.items()):
        tp_sched = df["tp_sched_raw"]
        tp_norm = tp_sched / df_hare["tp_base_raw"]

        if plot_min:
            idx = tp_norm.idxmin()
            tp_norm = tp_norm[[idx]]
            tp_sched = tp_sched[[idx]]

        rect = ax.bar(
            xs + method_idx * bar_width,
            tp_norm,
            width=bar_width,
            label=policy.plot_name,
            color=policy.color,
            # at some spacing at the edge
        )

        # label the bars with the throughput
        if not plot_min:
            ax.bar_label(
                rect,
                labels=[f"{t:.0f}" for t in tp_sched],
                fontsize=6,
                rotation=90,
                padding=1.5,
                annotation_clip=False,
            )

    # draw a dashed line at 100% and label it as the baseline at the end
    ax.axhline(y=1, color="0.25", linestyle=":", linewidth=0.8, zorder=-1)

    # label the baseline at the right end of the plot
    # if not plot_min:
    #     ax.text(
    #         xs[-1] + bar_width * 3,
    #         110,
    #         "Baseline",
    #         ha="left",
    #         va="bottom",
    #         fontsize=9,
    #         rotation=90,
    #     )

    max_y = max([(df["tp_sched_raw"] / df["tp_base_raw"]).max()
                 for df in policy_to_df.values()]) * 1.1
    ax.set_ylim(0, int(max_y) + 1)
    if plot_min:
        # x-axis
        ax.set_xticks([bar_width], ["Min"], fontweight="semibold")
        ax.set_xlim(-bar_width, bar_width * 3)
        # ax.set_facecolor("0.95")

        # y-axis
        ax.set_yticks([])

        # other
        ax.spines[['right', 'top', 'left']].set_visible(False)
    else:
        # x-axis
        # ax.set_xlabel("Tenants", labelpad=0)
        ax.set_xticks(xs + bar_width, app_names)
        ax.set_xlim(-bar_width * 1, xs[-1] + bar_width * 3)

        # y-axis
        # ax.tick_params(axis='y', rotation=90)
        ax.set_ylabel("Normalized tput")
        ax.yaxis.set_major_locator(plt.MultipleLocator(1))

        # other
        ax.spines[['right', 'top']].set_visible(False)

    return ax.get_legend_handles_labels()


class Resource(Enum):
    CACHE = "cache"
    BW = "bw"
    CPU = "cpu"

    @property
    def column_name(self):
        return {
            Resource.CACHE: "cache_mb",
            Resource.BW: "bw_mbps",
            Resource.CPU: "cpu_cnt",
        }[self]

    @property
    def label(self):
        return {
            Resource.CACHE: "Cache (GB)",
            Resource.BW: "Bandwidth (GB/s)",
            Resource.CPU: "Number of CPUs",
        }[self]

    @property
    def scaling_factor(self):
        return {
            Resource.CACHE: 1 / 1024,  # MB to GB
            Resource.BW: 1 / 1024,  # MB/s to GB/s
            Resource.CPU: 1,
        }[self]


def get_bar_start(col: List[float]):
    return [sum(col[:i]) for i in range(len(col))]


def plot_bars(res_to_ax, policy_to_df, app_names):
    bar_height = 0.4

    policy_to_df = dict(reversed(policy_to_df.items()))

    for (res, ax) in res_to_ax.items():
        for idx, (policy, df) in enumerate(policy_to_df.items()):
            is_drfnp_cache = policy == Policy.DRFNP and res == Resource.CACHE

            total_width = df[res.column_name].sum() * res.scaling_factor
            width = df[res.column_name] * res.scaling_factor if not is_drfnp_cache else total_width
            left = get_bar_start(width) if not is_drfnp_cache else [0]

            # plot stacked bars
            rects = ax.barh(
                idx * 0.5,
                width=width,
                left=left,
                height=bar_height,
                color=policy.color,
                edgecolor=policy.text_color,
                linewidth=1,
            )

            # add text labels for tenants
            for i, rect in enumerate(rects):
                # skip small bars
                w = rect.get_width() / total_width
                if w < 0.04:
                    continue
                text = app_names[i] if not is_drfnp_cache else "non-partitioned"
                ax.text(
                    rect.get_x() + rect.get_width() / 2,
                    rect.get_y(),
                    text,
                    ha="center",
                    va="bottom",
                    color=policy.text_color,
                    # fontsize=9,
                )

            # plot a border for the total width
            ax.barh(idx * 0.5, width=total_width, height=bar_height, fill=False)

        # x-axis
        xmax = max([rect.get_x() + rect.get_width() for rect in ax.patches])
        ax.set_xlim(0, xmax)
        ax.xaxis.set_major_locator(plt.MaxNLocator(4, min_n_ticks=4))
        ax.xaxis.set_major_formatter('{x:g}')
        ax.set_xlabel(res.label, labelpad=0)

        # y-axis
        ax.set_yticks([0, 0.5, 1])
        ax.set_yticklabels([k.plot_name for k in policy_to_df.keys()])
        ax.tick_params(axis='y', length=0)

        ax.spines[['left', 'right', 'top', 'bottom']].set_visible(False)


def get_app_short_name(s):
    # s = s.replace("YCSB", "").replace("-", "").replace("G", "")  # YCSB-C-0.5G -> C0.5
    # s = s.replace("M", "G")
    # idx = {
    #     "0.5": 1,
    #     "2": 2,
    # }[s[1:]]
    # return f"{s[0]}$_{{{idx}}}$"  # C0.5 -> $C_{1}$
    short_name_map = {
        "M0.5G": " $T_1$",
        "M2G": " $T_2$",
        "U0.5G": " $T_3$",
        "U2G": " $T_4$",
        "YCSB-C-0.5G": " $T_5$",
        "YCSB-C-2G": " $T_6$",
        "YCSB-E-0.5G": " $T_7$",
        "YCSB-E-2G": " $T_8$",
    }
    return short_name_map[s]


def plot_results(path: Path):
    # setup plot
    # fig, axes = plt.subplots(
    #     nrows=4, ncols=1, figsize=(5, 5),
    #     height_ratios=[2.25, 1, 1, 1], gridspec_kw={'hspace': 0.7},
    # )
    # (ax_tp, ax_cache, ax_bw, ax_cpu) = axes
    # fig = plt.figure(figsize=(4.5, 4.5), constrained_layout=True)
    # gs = fig.add_gridspec(4, 1, height_ratios=[2, 1, 1, 1], hspace=0.0)
    # gs_tp = gs[0].subgridspec(1, 2, width_ratios=[8, 1], wspace=0.0)
    # (ax_tp, ax_tp_min) = gs_tp.subplots()
    # (ax_cache, ax_bw, ax_cpu) = gs[1:].subgridspec(3, 1).subplots()

    # res_to_ax = {
    #     Resource.CACHE: ax_cache,
    #     Resource.BW: ax_bw,
    #     Resource.CPU: ax_cpu,
    # }
    fig = plt.figure(figsize=(SINGLE_COLUMN_WIDTH, SINGLE_COLUMN_WIDTH * 0.3), constrained_layout=True)
    gs = fig.add_gridspec(1, 1, hspace=0.0)
    gs_tp = gs[0].subgridspec(1, 2, width_ratios=[8, 1], wspace=0.0)
    (ax_tp, ax_tp_min) = gs_tp.subplots()

    # load data
    app_names = ["M0.5G", "M2G", "U0.5G", "U2G", "YCSB-C-0.5G", "YCSB-C-2G", "YCSB-E-0.5G", "YCSB-E-2G"]
    policy_to_df = {
        p: load_static_data(path / p.value).set_index("app").reindex(app_names).reset_index()
        for p in Policy if (path / p.value).exists()
    }
    app_names = [get_app_short_name(s) for s in app_names]

    # plot
    legend_handles_labels = plot_tp(ax_tp, policy_to_df, app_names)
    plot_tp(ax_tp_min, policy_to_df, app_names, plot_min=True)
    # plot_bars(res_to_ax, policy_to_df, app_names)

    export_fig(fig, path, "leveldb")
    save_legend(legend_handles_labels, path, "legend")


def main(paths):
    for p in paths:
        plot_results(resolve_output_dir(p))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--paths", type=str, default=["leveldb-1GB", "leveldb-2GB"], nargs="+")
    args = parser.parse_args()
    main(args.paths)
