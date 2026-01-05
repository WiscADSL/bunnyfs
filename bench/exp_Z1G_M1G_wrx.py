#!/usr/bin/env python3
from exp_utils import get_output_dir, prepare_output_dir
from exp_2apps import run_2apps_exp_wr, plot_2apps_exp_wr
from exp_2apps_utils import parse_write_rato_list_from_arg
from spec import *

if __name__ == "__main__":
    args = parse_write_rato_list_from_arg()

    static_name = "Tenant X"
    vary_name = "Tenant M"
    exp_name = "exp_Z1G_M1G_wrx"

    if not args.plot:
        run_2apps_exp_wr(output_dir=prepare_output_dir(exp_name),
                         static_off=OffsetType.ZIPF,
                         static_ws=1,
                         vary_off=OffsetType.UNIF,
                         vary_ws=1,
                         vary_wr_list=args.write_ratio_list,
                         static_name=static_name,
                         vary_name=vary_name,
                         overwrite=args.overwrite)

    plot_2apps_exp_wr(results_dir=get_output_dir(exp_name),
                      static_name=static_name,
                      vary_name=vary_name)
