from plot_var import legend_marker_size
from utils import PROJ_PATH

from plot_util import *


def make_legend(keys: List[str], result_dir, name):
    lines = [
        plt.plot(
            [], [],
            color=color_map[k],
            linestyle=linestyle_map[k],
            marker=marker_map[k],
            markersize=legend_marker_size
        )[0]
        for k in keys
    ]

    fig = plt.figure()
    legend = fig.legend(
        lines,
        [label_map[k] for k in keys],
        loc='center',
        ncol=len(keys),
        frameon=False,
        columnspacing=1.5,
        labelspacing=0.3,
        borderpad=0.0
    )
    bbox = legend.get_tightbbox().transformed(fig.dpi_scale_trans.inverted())
    export_fig(fig, result_dir, name, bbox_inches=bbox)


if __name__ == "__main__":

    make_legend(
        keys=["Tenant X", "Tenant U", "Tenant Z", "Tenant S", "Tenant M", "baseline"],
        result_dir=PROJ_PATH / "results",
        name="legend"
    )
