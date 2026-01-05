#!/usr/bin/env python3
from exp_2apps import run_2apps_exp_ws, plot_2apps_exp_ws
from exp_2apps_utils import parse_ws_list_from_arg
from exp_utils import prepare_output_dir, get_output_dir
from spec import *

if __name__ == "__main__":
    args = parse_ws_list_from_arg()
    ws_list = args.working_set_list

    static_name = "Tenant X"
    vary_name = "Tenant S"
    exp_name = "exp_Z1G_SxG"

    # here is a minor issue: because some metadata also takes space, the actual
    # working set is not strictly as what we provide. this is not a problem in
    # general, except at SEQ case where this could lead to a cliff...
    if 0.5 in ws_list:
        idx = ws_list.index(0.5)
        ws_list[idx] = 0.495

    if not args.plot:
        run_2apps_exp_ws(output_dir=prepare_output_dir(exp_name),
                         static_off=OffsetType.ZIPF,
                         static_ws=1,
                         vary_off=OffsetType.SEQ,
                         vary_ws_list=ws_list,
                         static_name=static_name,
                         vary_name=vary_name,
                         overwrite=args.overwrite)

    plot_2apps_exp_ws(results_dir=get_output_dir(exp_name),
                      static_name=static_name,
                      vary_name=vary_name)
