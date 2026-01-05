"""
This file contains utility functions related to 2-apps experiments. No main().
"""
import argparse


def parse_ws_list_from_arg():
    parser = argparse.ArgumentParser()
    parser.add_argument("working_set_list",
                        help="A list of working set sizes in GB",
                        type=float,
                        default=[round(0.1 * i, 2) for i in range(1, 21)],
                        nargs='*')
    parser.add_argument("--overwrite",
                        help="Write data to the existing directory",
                        action="store_true")
    parser.add_argument("--plot",
                        help="Only plot the data",
                        action="store_true")
    return parser.parse_args()


def parse_theta_list_from_arg():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "zipf_theta_list",
        help="A list of zipfian theta",
        type=float,
        default=[0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 0.99],
        nargs='*')
    parser.add_argument("--overwrite",
                        help="Write data to the existing directory",
                        action="store_true")
    parser.add_argument("--plot",
                        help="Only plot the data",
                        action="store_true")
    return parser.parse_args()


def parse_write_rato_list_from_arg():
    parser = argparse.ArgumentParser()
    parser.add_argument("write_ratio_list",
                        help="A list of working set sizes in GB",
                        type=float,
                        default=[0, 0.1, 0.2, 0.3, 0.4,
                                 0.5, 0.6, 0.7, 0.8, 0.9, 1],
                        nargs='*')
    parser.add_argument("--overwrite",
                        help="Write data to the existing directory",
                        action="store_true")
    parser.add_argument("--plot",
                        help="Only plot the data",
                        action="store_true")
    args = parser.parse_args()
    for wr in args.write_ratio_list:
        if wr > 1:
            raise ValueError("Write ratio must be <= 1")
    return args
