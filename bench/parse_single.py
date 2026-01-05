import re
from datetime import datetime
import pandas as pd
import numpy as np
from pathlib import Path


def parse_line(line):
    # Example: [2022-11-18 04:02:34.849] [App1-T1] [info] [stat.h:57] App1-T1-W1: Epoch 0: 10362 ops in 0.1 s (404.71 MB/s, 9.652 us/op)
    m = re.match(
        r"\[(?P<ts>.*)\] "
        r"\[(?P<thread>.*)\] "
        r"\[.*\] \[.*\] "  # ignore log level and file
        r"(?P<workload>.*): "
        r"Epoch\s*(?P<epoch>\d+): \d+ ops in \d+\.\d+ s "
        r"\(\s*(?P<throughput>\d+.\d+) (?P<tp_unit>.*),\s*(?P<latency>\d+.\d+) us/op\)",
        line,
    )
    if m is None:
        return None

    result = m.groupdict()
    result["app"] = result["thread"].rsplit("-", 1)[0]
    result["ts"] = datetime.strptime(result["ts"], "%Y-%m-%d %H:%M:%S.%f")
    result["throughput"] = float(result["throughput"])
    result["latency"] = float(result["latency"])
    result["epoch"] = int(result["epoch"])
    return result


def parse_results(path):
    """parse *.log into data frame"""
    results = []
    for file in path.iterdir():
        if file.suffix != ".log":
            continue

        with open(file, "r") as f:
            for line in f:
                result = parse_line(line)
                if result is not None:
                    results.append(result)

    if len(results) == 0:
        raise ValueError(f"No results found in {path}")

    df = pd.DataFrame(results)
    df["ts"] = (df["ts"] - df["ts"].min()) / pd.Timedelta(seconds=1)
    df["ts_sec"] = df["ts"].astype(int)
    return df


def app_summary(df):
    """Only for human to read"""
    return (
        df.groupby(["app", "ts_sec"])
        .agg({"throughput": [np.mean, np.sum], "latency": np.mean})
        .pivot_table(index="ts_sec", columns="app", values=["throughput", "latency"])
    )


def save_results(df, path):
    csv_path = path / "results.csv"
    df.to_csv(csv_path, index=False)
    print(f"Saving csv to {csv_path}")

    summary_path = path / "summary.txt"
    app_summary(df).to_string(summary_path)
    print(f"Saving summary to {summary_path}")


def parse_single(path: Path) -> pd.DataFrame:
    print(f"Reading result from {path}")
    df = parse_results(path)

    save_results(df, path)
    return df
