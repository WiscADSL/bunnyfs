"""
This file is used to programmatically generate the spec file in python.

The following class definitions allows better type checking and
auto-completion compared to manually writing the json file.
"""

import dataclasses
import json
from dataclasses import dataclass
from enum import Enum, unique
from pathlib import Path
from typing import List, Dict


@dataclass(frozen=True)
class PrepFile:
    path: str
    size: int

    def __post_init__(self):
        assert self.size > 0, f"{self.size} <= 0"


@dataclass(frozen=True)
class Database:
    path: str
    num_keys: int
    value_size: int

    def __post_init__(self):
        assert self.num_keys > 0, f"{self.num_keys} <= 0"
        assert self.value_size > 0, f"{self.value_size} <= 0"


@dataclass(frozen=True)
class Prep:
    databases: List[Database] = dataclasses.field(default_factory=list)
    files: List[PrepFile] = dataclasses.field(default_factory=list)


@unique
class OffsetType(str, Enum):
    ZIPF = "zipf"
    UNIF = "unif"
    SEQ = "seq"
    MIXGRAPH = "mixgraph"
    SHUFFLE = "shuffle"

    def short_name(self):
        if self == OffsetType.SHUFFLE:
            return "Sh"
        return self.value[0].upper()


@dataclass(frozen=True)
class Offset:
    type: OffsetType
    min: int
    max: int
    align: int
    theta: float

    def __post_init__(self):
        if self.type != OffsetType.ZIPF:
            assert self.theta == 0.0, f"{self.theta} != 0.0"
        assert self.min <= self.max, f"{self.min} > {self.max}"
        assert self.max % self.align == 0, f"{self.max} % {self.align} != 0"
        assert self.min % self.align == 0, f"{self.min} % {self.align} != 0"
        assert self.min >= 0, f"{self.min} < 0"
        assert self.max >= 0, f"{self.max} < 0"


@dataclass(frozen=True)
class Workload:
    name: str
    offset: Offset
    count: int
    qdepth: int = 1
    ops: int = 1 << 30
    duration_sec: int = 1 << 30
    read_ratio: float = 1.0
    dirty_threshold: int = 256 << 10

    def __post_init__(self):
        assert self.ops > 0, f"{self.ops} <= 0"
        assert self.duration_sec > 0, f"{self.duration_sec} <= 0"
        assert self.count > 0, f"{self.count} <= 0"


@unique
class ThreadType(str, Enum):
    RW = "rw"
    DB = "db"


@dataclass(frozen=True)
class Thread:
    type: ThreadType
    name: str
    worker_id: int
    core: int
    workloads: List[Workload]
    db_path: str = ""
    file_paths: List[str] = dataclasses.field(default_factory=list)
    pin_file_map: Dict[str, int] = dataclasses.field(default_factory=dict)

    def __post_init__(self):
        if self.type == ThreadType.DB:
            assert self.db_path != "", "db_path is empty"
            for w in self.workloads:
                assert w.qdepth == 1, "DB workload only supports sync APIs"
            # For DB workload, we allow `align` to be different from `count`
            # e.g., scan of length 50 can start anywhere in the DB
            # (except the last 50 keys)

        elif self.type == ThreadType.RW:
            num_files = len(self.file_paths)
            assert num_files != 0, "file_paths is empty"

        for w in self.workloads:
            if self.type == ThreadType.DB:
                assert w.qdepth == 1, "DB workload only supports sync APIs"


@dataclass(frozen=True)
class App:
    desc: str
    aid: int
    name: str
    threads: List[Thread]


@dataclass(frozen=True)
class Exp:
    num_workers: int
    prep: Prep
    apps: List[App]

    def __post_init__(self):
        # Check that all threads have valid worker_id
        for a in self.apps:
            for t in a.threads:
                assert (
                    t.worker_id < self.num_workers
                ), f"{t.worker_id} >= {self.num_workers} for {t.name}"

    def dump(self):
        return json.dumps(dataclasses.asdict(self), indent=2)

    def export_to_path(self, path):
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w") as f:
            f.write(self.dump())
            f.write("\n")
        print(f"Exported to {path}")
        return path

    def export_with_name(self, name):
        return self.export_to_path(self.get_path_from_name(name))

    @staticmethod
    def get_path_from_name(name):
        return Path(__file__).parent / "specs" / f"{name}.json"


__all__ = ["App", "Exp", "Thread", "ThreadType", "Workload",
           "Offset", "OffsetType", "Prep", "PrepFile", "Database"]
