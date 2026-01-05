#!/usr/bin/env python3
from dataclasses import dataclass
from functools import partial

from exp_leveldb_prep import VALUE_SIZE
from spec_app import WorkloadConfigPerApp, ExpConfig
from spec import *


@dataclass(frozen=True)
class DBWorkloadConfigPerApp(WorkloadConfigPerApp):
    # override some default values from WorkloadConfigPerApp
    qdepth: int = 1
    read_ratio: float = 1.0
    duration_sec: int = 30
    count: int = 1

    WRITE_AMP: float = 1.25

    def __post_init__(self):
        assert self.qdepth == 1, "DB workload only supports sync APIs"

    def get_workload(self, tid: int, num_threads: int, num_files: int):
        num_keys = int(self.working_set_gb * 1024 ** 3 / VALUE_SIZE / num_threads / self.WRITE_AMP)
        if self.count > 1:
            num_keys -= (self.count - 1)  # to avoid scan beyond the end of the DB

        return Workload(
            name=self.name,
            duration_sec=self.duration_sec,
            offset=Offset(
                type=self.offset_type,
                min=0,
                max=num_keys,
                align=1,
                theta=self.zipf_theta if self.offset_type == OffsetType.ZIPF else 0.0,
            ),
            count=self.count,
            read_ratio=self.read_ratio,
        )


app_dicts = {
    f"{k}{ws}G": v(working_set_gb=ws)
    for k, v in {
        "M": partial(DBWorkloadConfigPerApp, offset_type=OffsetType.MIXGRAPH),
        "A": partial(DBWorkloadConfigPerApp, offset_type=OffsetType.ZIPF, zipf_theta=0.5, read_ratio=0.5),
        "C": partial(DBWorkloadConfigPerApp, offset_type=OffsetType.ZIPF, zipf_theta=0.5),
        "E": partial(DBWorkloadConfigPerApp, offset_type=OffsetType.ZIPF, zipf_theta=0.5, count=50),
    }.items()
    for ws in [0.5, 2]
}


def gen_leveldb_spec() -> Exp:
    exp_config = ExpConfig(
        num_workers=4,
        num_apps=len(app_dicts),
        num_threads_per_app=4,
        num_files_per_app=0,
        use_affinity=True,
        is_symm=True,
        thread_type=ThreadType.DB,
    )

    return exp_config.get_exp([
        exp_config.get_app(aid, name, wl)
        for (aid, (name, wl)) in enumerate(app_dicts.items())
    ])


if __name__ == "__main__":
    exp = gen_leveldb_spec()
    exp.export_with_name("leveldb")
