import logging
from typing import List
import matplotlib.pyplot as plt

logging.getLogger("fontTools.subset").level = logging.WARN

plt.rcParams["pdf.fonttype"] = 42
plt.rcParams["ps.fonttype"] = 42
plt.rcParams["font.size"] = 7
plt.rcParams["xtick.major.pad"] = "2"
plt.rcParams["ytick.major.pad"] = "2"
plt.rcParams["xtick.major.size"] = "2.5"
plt.rcParams["ytick.major.size"] = "2.5"
plt.rcParams["axes.labelpad"] = "1"

plt.rcParams["mathtext.default"] = "regular"


DOUBLE_COLUMN_WIDTH = 7
COLUMN_SEP = 0.33
SINGLE_COLUMN_WIDTH = (DOUBLE_COLUMN_WIDTH - COLUMN_SEP) / 2


color_map = {
    "Tenant X": "0",
    "Tenant U": "0.6",
    "Tenant Z": "0.4",
    "Tenant S": "0.8",
    "Tenant W": "0.5",
    "Tenant M": "0.5",
    "expected_improv": "0.2",
    "baseline": "0",
}

marker_map = {
    "Tenant X": "o",
    "Tenant U": "d",
    "Tenant Z": "^",
    "Tenant S": "s",
    "Tenant W": "p",
    "Tenant M": "p",
    "expected_improv": None,
    "baseline": None,
}

linestyle_map = {
    "Tenant X": "-",
    "Tenant U": "-",
    "Tenant Z": "-",
    "Tenant S": "-",
    "Tenant W": "-",
    "Tenant M": "-",
    "expected_improv": "--",
    "baseline": ":",
}

label_map = {
    "Tenant X": "Tenant X (Zipf-0.99, 1GB)",
    "Tenant U": "Tenant U (Unif)",
    "Tenant Z": "Tenant Z (Zipf, 2GB)",
    "Tenant S": "Tenant S (Seq)",
    "Tenant W": "Tenant W (Unif, 50% read)",
    "Tenant M": "Tenant M (Unif, 1GB, rw)",
    "expected_improv": "Expected",
    "baseline": "Baseline",
}


def export_fig(fig, dir, name, bbox_inches='tight'):
    for ext in ["png", "pdf"]:
        path = dir / f"{name}.{ext}"
        fig.savefig(path, dpi=300, bbox_inches=bbox_inches, pad_inches=0.01)
        print(f"Figure saved to {path}")


def save_legend(legend_handles_labels, path, name):
    fig, ax = plt.subplots(1, 1, figsize=(SINGLE_COLUMN_WIDTH, SINGLE_COLUMN_WIDTH * 0.05))
    ax.axis("off")
    legend = ax.legend(
        *legend_handles_labels,
        loc="upper center",
        ncol=len(legend_handles_labels[0]),
        frameon=False,
        borderpad=0.0
    )
    bbox = legend.get_tightbbox().transformed(fig.dpi_scale_trans.inverted())
    export_fig(fig, path, name, bbox_inches=bbox)


def get_cdf(data: List[float]):
    x = sorted(data)
    y = [i / (len(data) - 1) for i in range(len(data))]
    return x, y
