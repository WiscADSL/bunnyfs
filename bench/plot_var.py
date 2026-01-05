from pathlib import Path
import logging

from typing import List
import matplotlib
import matplotlib.pyplot as plt

from plot_util import *

impr_ticks = [1, 1.5, 2]
cache_ticks = [0, 0.5, 1]
bw_ticks = [0, 1, 2]
cpu_ticks = [0, 1, 2, 3, 4]
tp_ticks1 = [0, 4, 8, 12, 16]
tp_ticks2 = [0, 2, 4, 6, 8]

matplotlib.rcParams['pdf.fonttype'] = 42
matplotlib.rcParams['ps.fonttype'] = 42

marker_size = 3
legend_marker_size = 5


# the default values below are varying-working-set-oriented; should provide
# other values manully for varying-theta experiments
def plot_var(
    df,
    *,
    vary_name,
    expected_df=None,
    xlabel: str,
    results_dir: Path,
):
    # temporally disable logger
    # we don't want to see too much internal logging from matplotlib
    logger = logging.getLogger()
    prev_logging_level = logger.level
    logger.setLevel(logging.WARNING)

    if vary_name == "Tenant Z":
        xticks = [0, 0.5, 0.99]
        xlim = [-0.05, 1.05]
    elif vary_name == "Tenant M":
        xticks = [0, 0.5, 1]
        xlim = [-0.05, 1.05]
    else:
        xticks = [0.1, 1, 2]
        xlim = [0, 2.1]

    fig, axes = plt.subplots(nrows=2, ncols=3)
    fig.set_size_inches(5.25, 3.5)
    axes = axes.flatten()
    (ax_impr, ax_tp1, ax_tp2, ax_cache, ax_bw, ax_cpu) = axes

    df = df.sort_values(by="var")
    for y_col, ax in [("tp_norm", ax_impr), ("cache_gb", ax_cache),
                      ("bw_gbps", ax_bw), ("cpu_cnt", ax_cpu)]:
        for app, d in df.groupby("app"):
            ax.plot(d["var"],
                    d[y_col],
                    f"{marker_map[app]}{linestyle_map[app]}",
                    color=color_map[app],
                    label=label_map[app],
                    markersize=marker_size)
            ax_tp = ax_tp2 if app == vary_name else ax_tp1
            ax_tp.plot(d["var"], d["tp_sched"],
                       f"{marker_map[app]}{linestyle_map[app]}",
                       color=color_map[app],
                       label=label_map[app],
                       markersize=marker_size)
            ax_tp.plot(d["var"], d["tp_base"], linestyle_map["baseline"],
                       color=color_map[app],
                       label=label_map[app])

    if expected_df is not None:
        expected_df = expected_df.sort_values(by="var")
        ax_impr.plot(expected_df["var"],
                     expected_df["expected_improv"] / 100,
                     linestyle_map["expected_improv"],
                     color=color_map["expected_improv"])

    # set the ticks and limits
    for ax, ticks in zip([ax_impr, ax_tp1, ax_tp2,  ax_cache, ax_bw, ax_cpu],
                         [impr_ticks, tp_ticks1, tp_ticks2, cache_ticks, bw_ticks, cpu_ticks]):
        ax.set_xticks(xticks, [f"{t:.1f}" if t != 0.99 else f"{t:.2f}"
                               for t in xticks])
        ax.set_xlim(xlim)
        ax.set_yticks(
            ticks,
            rotation=90,
            labels=[
                f"{t * 100:.0f}" if ax is ax_impr else
                f"{t}" if type(t) is int else f"{t:.1f}"
                for t in ticks
            ],
            va="center"
        )
        ax.set_ylim(ticks[0], ticks[-1])

    ax_impr.set_ylabel("Normalized tput (%)")
    ax_tp1.set_ylabel("Tenant X tput (GB/s)")
    ax_tp2.set_ylabel(f"{vary_name} tput (GB/s)")
    ax_cache.set_ylabel("Cache (GB)")
    ax_bw.set_ylabel("Bandwidth (GB/s)")
    ax_cpu.set_ylabel("Number of CPUs")

    for ax in axes:
        ax.spines[['right', 'top']].set_visible(False)
        # ax.set_xlabel(xlabel)

    for ax in [ax_tp1, ax_bw]:
        ax.set_xlabel(xlabel.replace("\n", " "))

    # fig.supxlabel(xlabel.replace("\n", " "), fontsize=None)

    fig.set_tight_layout({"pad": 0.05, "w_pad": 0.2, "h_pad": 0.05})
    fig.savefig(results_dir / "result.jpg", dpi=300)
    print(f"Figure saved to {results_dir / 'result.jpg'}")
    fig.savefig(results_dir / "result.pdf")
    print(f"Figure saved to {results_dir / 'result.pdf'}")
    plt.close(fig)

    from plot_legend import make_legend
    make_legend(keys=["Tenant X", vary_name, "baseline"],
                result_dir=results_dir, name="legend")

    logger.setLevel(prev_logging_level)
