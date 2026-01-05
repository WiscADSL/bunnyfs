"""
This script generates the spec files for the 2-apps experiments.
"""

from spec import *
from spec_app import export_spec


def export_2apps_spec(
    exp_name,
    off1,
    ws1,
    off2,
    ws2,
    *,
    zipf_theta1=None,  # only valid if off1 is ZIPF
    zipf_theta2=None,  # only valid if off2 is ZIPF
    read_ratio1=1.0,
    read_ratio2=1.0,
    name1=None,
    name2=None,
    total_num_workers=4,
    num_threads_per_app=4,
    num_files_per_app=32,
    qdepth=8,
    use_affinity=True,
    is_symm=True,
):
    if zipf_theta1 is None and off1 == OffsetType.ZIPF:
        zipf_theta1 = 0.99
    if zipf_theta2 is None and off2 == OffsetType.ZIPF:
        zipf_theta2 = 0.99
    return export_spec(exp_name=exp_name,
                       app_configs=[(off1, ws1, read_ratio1, zipf_theta1, name1),
                                    (off2, ws2, read_ratio2, zipf_theta2, name2)],
                       total_num_workers=total_num_workers,
                       num_threads_per_app=num_threads_per_app,
                       num_files_per_app=num_files_per_app,
                       qdepth=qdepth,
                       use_affinity=use_affinity,
                       is_symm=is_symm)
