from typing import Tuple

import pandas as pd
from parse_static import load_throughput, load_alloc, load_expect, merge_alloc, convert_df


def save_df(df, results_dir, name):
    for fn, ext in [(df.to_csv, "csv"), (df.to_string, "txt")]:
        path = results_dir / f"{name}.{ext}"
        fn(path, index=False)
        print(f"Dataframe saved to {path}")


def parse_var(results_dir, var_name, var_list, apps_names) -> Tuple[pd.DataFrame, pd.DataFrame]:
    # apps_names is an ORDERED list: apps_names[i] -> App-i
    # parse experiments with single varying parameter
    results = []
    expected_results = []
    for var in var_list:
        subdir = results_dir / str(var)
        tp_base_list, tp_sched_list = load_throughput(f"{subdir}/results.csv")
        app_alloc_map = load_alloc(f"{subdir}/alloc.txt")
        # app_map corresponds to a row in the dataframe (one app a row)
        # however, the goal is not to show the info of each app but instead
        # labelled the key variable in this experiement (ie, position in x-axis)
        app_map = {
            app: {
                "var": float(var),  # labelled the key variable
                "var_name": var_name,  # keep a name for readability
                "app": app,
                "tp_base": tp,
            }
            for app, tp in tp_base_list
        }
        for app, tp in tp_sched_list:
            app_map[app]["tp_sched"] = tp
        merge_alloc(apps_names, app_map, app_alloc_map)
        results.extend(app_map.values())

        expected_improv = load_expect(f"{subdir}/expect.txt")
        expected_results.append({
            "var": float(var),
            "var_name": var_name,
            "expected_improv": float(expected_improv),
        })
    df = convert_df(pd.DataFrame(results))
    exp_df = pd.DataFrame(expected_results)

    save_df(df, results_dir, "data")
    save_df(exp_df, results_dir, "expected_data")

    return df, exp_df
