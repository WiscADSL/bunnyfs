#!/usr/bin/env python3

import matplotlib.pyplot as plt
from typing import Optional, Union
from pathlib import Path
import pandas as pd
from parse_single import parse_single


def plot_single(df_detailed: pd.DataFrame, path: Optional[Path] = None) \
        -> Union[plt.Figure, Path]:
    # df_detailed includes time-scale info (from results.csv)
    fig, axs = plt.subplots(2, 1, sharex=True)
    fig.subplots_adjust(hspace=0.3)
    for ax, ax_name in zip(axs, ("throughput", "latency")):
        # do not plot the first 5 epoch for latency as it is too large due to the warmup
        if ax_name == "latency":
            df_detailed = df_detailed[df_detailed["epoch"] > 5]
            ax.set_ylabel("Latency (us)")
        else:
            tp_unit = df_detailed["tp_unit"].unique()
            assert len(tp_unit) == 1
            ax.set_ylabel(f"Thread Throughput ({tp_unit[0]})")

        for (app, df_app), color in zip(df_detailed.groupby("app"), plt.cm.tab10.colors):
            # plot each thread
            for thread, df_thread in df_app.groupby("thread"):
                ax.plot(df_thread["epoch"],
                        df_thread[ax_name], label=app, color=color)

            # plot an average line across all threads
            df_avg = df_app.groupby("epoch").agg({ax_name: "mean"})
            ax.plot(df_avg.index, df_avg[ax_name],
                    label=app, color="black", ls="--")

            # annotate the label at the first and last point of df_avg
            for idx in (0, -1):
                ax.annotate(
                    app,
                    (df_avg.index[idx], df_avg.iloc[idx][ax_name]),
                    ha="center",
                    va="bottom",
                )

        ax.tick_params(labelbottom=True)
        ax.set_ylim(bottom=0)
        ax.set_xlabel("Time (s)")

    if path is None:  # don't save; caller may want to more edit
        return fig

    fig_path = path / "results.png"
    fig.savefig(fig_path, bbox_inches="tight", dpi=300)
    plt.close(fig)
    return fig_path


def parse_and_plot_single(path: Union[str, Path]):
    if isinstance(path, str):
        path = Path(path)
    df = parse_single(path)
    plot_single(df, path)
    return df
