from pathlib import Path

from typing import List, Tuple, Optional
import matplotlib
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np
import pandas as pd
from plot_util import *

color_map = {
    "base": "0.1",
    "drf": "0.5",
    "drfnp": "0.75",
    "hare": "0",
    "Tenant-Z1": "0",
    "Tenant-U": "0.25",
    "Tenant-S": "0.75",
    "Tenant-Z2": "0.5",
}

linestyle_map = {
    "base": ":",
    "drf": "--",
    "drfnp": "-.",
    "hare": "-",
    "Tenant-Z1": "--",
    "Tenant-U": "-",
    "Tenant-S": "-",
    "Tenant-Z2": "--",
}

label_map = {
    "base": "Base",
    "drf": "DRF",
    "drfnp": "NonPartCache+DRF",
    "hare": "HARE",
    "Tenant-U": "$T_1$",
    "Tenant-Z1": "$T_2$",
    "Tenant-Z2": "$T_3$",
    "Tenant-S": "$T_4$",
}

max_duration = 70
xticks = [0, 5, 10, 15, 20, 25, 30, 35, 40, 45]
xshift = 19
ts_list = [x + xshift for x in xticks]
tenant_list = ["Tenant-U", "Tenant-Z1", "Tenant-Z2", "Tenant-S"]


def preprocess(df: pd.DataFrame) -> pd.DataFrame:
    # drop unused columns
    df = df[["app", "ts_sec", "workload", "throughput"]]
    df = df[df["ts_sec"] < max_duration]
    return df.groupby(["app", "ts_sec", "workload"]).sum().reset_index()


def get_norm_base(df_base: pd.DataFrame, app: str, avg_window=4):
    def get_avg(df) -> float:
        avg_ = df["throughput"].mean()
        max_ = df["throughput"].max()
        min_ = df["throughput"].min()
        if (max_ - min_) / avg_ > 0.05:
            logging.warning(
                f"{app} may has unstable throughput: {df[['throughput']].to_numpy()}"
            )
        return avg_

    # pick the last 5s average throughput of the window as the baseline throughput of the window
    assert len(ts_list) > 0
    df_base_app = df_base[(df_base["app"] == app)]

    max_ts = df_base_app["ts_sec"].max()
    if max_ts > ts_list[-1]:
        ts_list.append(max_ts + 1)

    df = df_base_app[df_base_app["ts_sec"] < ts_list[0]].tail(avg_window)
    avg_ = get_avg(df)
    res = [avg_] * ts_list[0]

    for i in range(len(ts_list) - 1):
        ts_begin, ts_end = ts_list[i], ts_list[i + 1]
        df = df_base_app[
            (df_base_app["ts_sec"] >= ts_begin) & (df_base_app["ts_sec"] < ts_end)
        ].tail(avg_window)
        avg_ = get_avg(df)
        res.extend([avg_] * (ts_end - ts_begin))
    return np.array(res)


def plot_dynamic_tp_norm(
    ax,
    df: pd.DataFrame,
    df_base: pd.DataFrame,
    app_list: List[str],
    yticks=[0, 0.5, 1, 1.5, 2],
):
    for app in app_list:
        tp_base = get_norm_base(df_base, app)
        tp_abs = df[(df["app"] == app)]["throughput"]
        tp_norm = tp_abs / tp_base
        ax.plot(
            list(range(max_duration)),
            tp_norm,
            label=app,
            color=color_map[app],
            linestyle=linestyle_map[app],
        )

    for ts in ts_list:
        ax.axvline(
            xticks[0] + xshift + ts,
            color="black",
            linestyle=":",
            alpha=0.3,
            linewidth=0.5,
        )

    ax.set_xticks([t + xshift for t in xticks], xticks)
    ax.set_xlim(xticks[0] + xshift, xticks[-1] + xshift)
    ax.set_xlabel("Time (s)")

    ax.set_ylim(yticks[0], yticks[-1])
    # ax.tick_params(axis="y", rotation=90)
    ax.set_yticks(yticks, [f"{t * 100:g}" for t in yticks])
    ax.set_ylabel("Normalized\nThroughput (%)")
    ax.spines[["right", "top"]].set_visible(False)


def plot_dynamic_tp_abs(ax, df: pd.DataFrame, app_list: List[str]):
    for app in app_list:
        tp_abs = df[(df["app"] == app)]["throughput"]
        ax.plot(
            list(range(max_duration)),
            tp_abs,
            label=app,
            color=color_map[app],
            linestyle=linestyle_map[app],
        )

    for ts in ts_list:
        ax.axvline(
            xticks[0] + xshift + ts,
            color="black",
            linestyle=":",
            alpha=0.3,
            linewidth=0.5,
        )

    ax.set_xticks([t + xshift for t in xticks], xticks)
    ax.set_xlim(xticks[0] + xshift, xticks[-1] + xshift)
    ax.set_xlabel("Time (s)")

    # ax.set_ylim(0, 2048)
    # yticks = [0, 0.5, 1, 1.5, 2]
    # ax.tick_params(axis="y", rotation=90)
    # ax.set_yticks(yticks, [f"{t * 100:g}" for t in yticks])
    ax.set_ylabel("Absolute\nThroughput (MB/s)")
    ax.spines[["right", "top"]].set_visible(False)


def plot_dynamic_by_policy(
    *,
    df: pd.DataFrame,
    df_base: Optional[pd.DataFrame],
    policy_name: str,
    results_dir: Path,
):
    fig, ax = plt.subplots(nrows=1, ncols=1)
    fig.set_size_inches(SINGLE_COLUMN_WIDTH, SINGLE_COLUMN_WIDTH * 0.25)

    df = preprocess(df)
    if df_base is not None:
        df_base = preprocess(df_base)
        plot_dynamic_tp_norm(ax, df, df_base, tenant_list)
    else:
        plot_dynamic_tp_abs(ax, df, tenant_list)

    fig.set_tight_layout({"pad": 0.05, "w_pad": 0.0, "h_pad": 0.0})
    export_fig(
        fig,
        dir=results_dir,
        name="result_dynamic_"
        f"{'norm' if df_base is not None else 'abs'}_{policy_name}",
    )
    plt.close(fig)


def plot_dynamic_by_tenant(
    *,
    df_base: pd.DataFrame,
    df_hare: pd.DataFrame,
    df_drf: pd.DataFrame,
    df_drfnp: pd.DataFrame,
    results_dir: Path,
):
    fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(nrows=2, ncols=2)
    axes = [ax1, ax2, ax3, ax4]
    fig.set_size_inches(SINGLE_COLUMN_WIDTH, SINGLE_COLUMN_WIDTH / 2)
    for ax, (tenant_name, yticks) in zip(
        axes,
        [
            ("Tenant-U", [0, 1, 2, 3]),
            ("Tenant-Z1", [0, 2, 4, 6]),
            ("Tenant-Z2", [0, 1, 2, 3, 4]),
            ("Tenant-S", [0, 2, 4, 6]),
        ],
    ):
        # use this plot order to ensure every line is visible
        for policy_name, df in [
            ("hare", df_hare),
            ("drfnp", df_drfnp),
            ("drf", df_drf),
            ("base", df_base),
        ]:
            df = preprocess(df)
            tp_abs = df[(df["app"] == tenant_name)]["throughput"]
            ax.plot(
                list(range(max_duration)),
                tp_abs,
                linewidth=0.8,
                label=policy_name,
                color=color_map[policy_name],
                linestyle=linestyle_map[policy_name],
            )

        # enable allocator
        # ax.axvline(xticks[0] + xshift + 10, color="black", linestyle=":", alpha=0.3, linewidth=0.5)
        # ax.text(
        #     xticks[0] + xshift + 10 + 1.5,
        #     yticks[-1] * 1024 * 0.94,
        #     "start",
        #     fontsize=7,
        #     ha="left",
        #     va="center",
        #     zorder=-10,
        #     # bbox={
        #     #     "facecolor": "0.9",
        #     #     "edgecolor": "none",
        #     #     "alpha": 0.5,
        #     #     "boxstyle": "round,pad=0.15",
        #     # },
        # )
        # ax.plot(
        #     xticks[0] + xshift + 10,
        #     yticks[-1] * 1024 * 0.94,
        #     marker=9,
        #     color="black",
        #     markersize=5,
        #     markerfacecolor="0.2",
        #     markeredgecolor="none",
        #     clip_on=False,
        # )

        ax.set_xticks([t + xshift for t in xticks], xticks)
        ax.set_xlim(xticks[0] + xshift, xticks[-1] + xshift)
        if ax == axes[-1] or ax == axes[-2]:
            ax.set_xlabel("Time (s)")

        ax.set_ylim(yticks[0] * 1024, yticks[-1] * 1024)
        # ax.tick_params(axis="y", rotation=90)
        ax.set_yticks([t * 1024 for t in yticks], [f"{t:g}" for t in yticks])
        ax.set_ylabel(f"{label_map[tenant_name]} tput (GB/s)")
        ax.spines[["right", "top"]].set_visible(False)

        # ax.yaxis.set_label_coords(-0.042, 0.4)
        # ax.text(xticks[0] + xshift + 1, yticks[-1] * 1024 * 0.9, tenant_name,
        #         ha='left', va='center')

        def label_workload(ax, ts: float, wl: str):
            ax.axvline(
                xticks[0] + xshift + ts,
                color="black",
                linestyle=":",
                alpha=0.3,
                linewidth=0.5,
            )
            # ax.text(
            #     xticks[0] + xshift + ts - 1,
            #     yticks[-1] * 1024 * 1.1,
            #     wl,
            #     fontsize=7,
            #     ha="left",
            #     va="center",
            #     zorder=-10,
            #     bbox={
            #         "facecolor": "0.9",
            #         "edgecolor": "none",
            #         "alpha": 0.5,
            #         "boxstyle": "round,pad=0.15",
            #     },
            # )
            ax.plot(
                xticks[0] + xshift + ts,
                yticks[-1] * 1024,
                marker=11,
                color="black",
                markersize=5,
                markerfacecolor="0.2",
                markeredgecolor="none",
                clip_on=False,
            )

        if tenant_name == "Tenant-U":
            # label_workload(ax, 0, "unif,2G")
            # label_workload(ax, 5, "unif,1G")
            label_workload(ax, 5, "unif,[2G->1G]")
        elif tenant_name == "Tenant-Z1":
            # label_workload(ax, 0, "zipf-0.99,2G")
            # label_workload(ax, 15, "zipf-0.99,1G")
            label_workload(ax, 15, "zipf:0.99,[2G->1G]")
        elif tenant_name == "Tenant-Z2":
            # label_workload(ax, 0, "zipf-0.5,1.5G")
            # label_workload(ax, 25, "zipf-0.99,1.5G")
            label_workload(ax, 25, "zipf:[0.5->0.99],1.5G")
        elif tenant_name == "Tenant-S":
            # label_workload(ax, 0, "seq,2G")
            # label_workload(ax, 35, "seq,0.5G")
            label_workload(ax, 35, "seq,[2G->0.5G]")

    last_ax = axes[-1]
    last_tenant = "Tenant-S"

    zoom_xmin = 19
    zoom_xmax = 21
    zoom_ymin = 0.4
    zoom_ymax = 0.8

    ax_zoom = last_ax.inset_axes(
        [
            12 + xshift,  # x
            1024 * 2,  # y
            16,  # width
            3500,  # height
        ],
        transform=last_ax.transData,
        xlim=(zoom_xmin + xshift, zoom_xmax + xshift),
        ylim=(1 * 1024, 0.2 * 1024),  # intentially swapped to make the lines correct
    )
    ax_zoom.set_facecolor("0.95")
    (patch, lines) = last_ax.indicate_inset_zoom(
        ax_zoom,
        facecolor="0.95",
        edgecolor="0",
        # linestyle="--",
        linewidth=0.5,
    )
    for l in lines:
        l.set_linewidth(0.2)
        # l.set_linestyle("--")

    ax_zoom.set_xlim(zoom_xmin, zoom_xmax)
    # ax_zoom.set_xticks([zoom_xmin, zoom_xmax])
    ax_zoom.set_xticks([])

    ax_zoom.set_ylim(zoom_ymin, zoom_ymax)
    ax_zoom.set_yticks(np.arange(zoom_ymin, zoom_ymax + 0.2, 0.2))

    ax_zoom.tick_params(axis="both", labelsize=6, length=2, width=0.2)
    # ax_zoom.spines[["right", "top"]].set_visible(False)
    for spine in ax_zoom.spines.values():
        spine.set_linewidth(0.2)
        # spine.set_visible(False)

    for policy_name, df in [
        ("hare", df_hare),
        ("drfnp", df_drfnp),
        ("drf", df_drf),
        ("base", df_base),
    ]:
        df = preprocess(df)
        tp_abs = df[(df["app"] == last_tenant)]["throughput"]
        ax_zoom.plot(
            np.arange(zoom_xmin, zoom_xmax + 1),
            tp_abs[zoom_xmin + xshift : zoom_xmax + xshift + 1] / 1024,
            color=color_map[policy_name],
            linestyle=linestyle_map[policy_name],
            clip_on=False,
            linewidth=1.0,
        )

    fig.set_tight_layout({"pad": 0.0, "w_pad": 0.0, "h_pad": 0.2})
    export_fig(fig, dir=results_dir, name="result_dynamic_tenants")
    plt.close(fig)


def make_dynamic_legend(
    fname: str,
    key_list: List[str],
    width: float,
    height: float,
    fontsize: float,
    columnspacing: float,
):
    pseudo_fig = plt.figure()
    ax = pseudo_fig.add_subplot(111)

    lines = []
    for k in key_list:
        (line,) = ax.plot(
            [], [], color=color_map[k], linestyle=linestyle_map[k], label=label_map[k]
        )
        lines.append(line)

    legend_fig = plt.figure()
    legend_fig.set_size_inches(width, height)
    legend_fig.legend(
        lines,
        [label_map[k] for k in key_list],
        loc="center",
        ncol=len(key_list),
        fontsize=fontsize,
        frameon=False,
        columnspacing=columnspacing,
        labelspacing=0.4,
    )
    legend_fig.set_tight_layout({"pad": 0, "w_pad": 0, "h_pad": 0})
    print(f"Legend saved to {fname}.png")
    legend_fig.savefig(f"{fname}.png")
    print(f"Legend saved to {fname}.pdf")
    legend_fig.savefig(f"{fname}.pdf")
    plt.close(legend_fig)


def make_dynamic_legend_tenant(
    fname: str,
    width: float = 5,
    height: float = 0.15,
    fontsize: float = 10,
    columnspacing: float = 1.7,
):
    make_dynamic_legend(
        fname=fname,
        key_list=["Tenant-U", "Tenant-Z1", "Tenant-Z2", "Tenant-S"],
        width=width,
        height=height,
        fontsize=fontsize,
        columnspacing=columnspacing,
    )


def make_dynamic_legend_policy(
    fname: str,
    width: float = 5,
    height: float = 0.15,
    fontsize: float = 10,
    columnspacing: float = 1.7,
):
    make_dynamic_legend(
        fname=fname,
        key_list=["base", "drf", "drfnp", "hare"],
        width=width,
        height=height,
        fontsize=fontsize,
        columnspacing=columnspacing,
    )
