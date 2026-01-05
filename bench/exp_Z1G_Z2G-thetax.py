#!/usr/bin/env python3
from exp_utils import get_output_dir, prepare_output_dir
from exp_2apps import run_2apps_exp_theta, plot_2apps_exp_theta
from exp_2apps_utils import parse_theta_list_from_arg
from spec import *

if __name__ == "__main__":
    args = parse_theta_list_from_arg()

    static_off = OffsetType.ZIPF
    static_name = "Tenant X"
    vary_name = "Tenant Z"
    exp_name = "exp_Z1G_Z2G-thetax"

    if not args.plot:
        run_2apps_exp_theta(output_dir=prepare_output_dir(exp_name),
                            static_off=static_off,
                            static_ws=1,
                            vary_ws=2,
                            vary_theta_list=args.zipf_theta_list,
                            static_name=static_name,
                            vary_name=vary_name)

    plot_2apps_exp_theta(results_dir=get_output_dir(exp_name),
                         static_name=static_name,
                         vary_name=vary_name)
