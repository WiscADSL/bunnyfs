# BunnyFS

BunnyFS is a multi-tenant filesystem semi-microkernel that extends uFS with the *HARE* algorithm, which jointly allocates page cache and CPU cycles across tenants. You can learn more details about the HARE algorithm and BunnyFS in the FAST'26 paper [*Cache-Centric Multi-Resource Allocation for Storage Services*](https://www.usenix.org/conference/fast26/presentation/ye).

**Please refer to [`ARTIFACT.md`](./ARTIFACT.md) for instructions to reproduce experiments.**

The original uFS README can be found in [`README_uFS.md`](./README_uFS.md).

The major code changes BunnyFS made to uFS includes:
- `cfs/sched/`: allocation module runs the HARE algorithm.
- more fine-grained queuing and scheduling control for fs worker.
- multi-tenant page cache based on [gcache](https://github.com/chenhao-ye/gcache/).
- `bench/`: scheduling-specific benchmarks.
