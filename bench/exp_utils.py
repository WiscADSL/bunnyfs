"""
This file contains utility functions for experiments.
"""

from datetime import datetime
import logging

from pathlib import Path
from utils import PROJ_PATH


def get_subdir_names(dir_path):
    return [d.name for d in sorted(dir_path.iterdir()) if d.is_dir()]


def resolve_output_dir(name_or_path: str) -> Path:
    p = Path(name_or_path)
    if p.is_dir() and p.is_absolute():
        return p
    else:
        return get_output_dir(name_or_path)


def get_output_dir(exp_name, resolve=True) -> Path:
    p = PROJ_PATH / "results" / exp_name
    return p.resolve() if resolve else p


def prepare_output_dir(exp_name, overwrite=False) -> Path:
    # create a symlink to the actual output dir with a fixed name
    output_dir = get_output_dir(exp_name, resolve=False)
    if output_dir.exists():
        if overwrite:
            logging.info(f"Detect {output_dir} exists. Will reuse directory.")
            return output_dir
        else:
            # we only remove the symbolic link
            output_dir.unlink()

    # prepare an actual output dir with a timestamp in its name
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    actual_output_dir = get_output_dir(f"{exp_name}_{ts}")
    actual_output_dir.mkdir(parents=True, exist_ok=True)

    output_dir.symlink_to(actual_output_dir, target_is_directory=True)
    return actual_output_dir
