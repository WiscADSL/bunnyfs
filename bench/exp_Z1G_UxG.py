#!/usr/bin/env python3
from exp_utils import get_output_dir, prepare_output_dir
from exp_2apps import run_2apps_exp_ws, plot_2apps_exp_ws
from exp_2apps_utils import parse_ws_list_from_arg
from spec import *

if __name__ == "__main__":
    args = parse_ws_list_from_arg()

    static_name = "Tenant X"
    vary_name = "Tenant U"
    exp_name = "exp_Z1G_UxG"

    if not args.plot:
        run_2apps_exp_ws(output_dir=prepare_output_dir(exp_name),
                         static_off=OffsetType.ZIPF,
                         static_ws=1,
                         vary_off=OffsetType.UNIF,
                         vary_ws_list=args.working_set_list,
                         static_name=static_name,
                         vary_name=vary_name,
                         overwrite=args.overwrite)

    plot_2apps_exp_ws(results_dir=get_output_dir(exp_name),
                      static_name=static_name,
                      vary_name=vary_name)
