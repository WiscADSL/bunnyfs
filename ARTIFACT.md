# BunnyFS Artifact

This document describes the instructions to reproduce the BunnyFS experiments in the FAST'26 paper [*Cache-Centric Multi-Resource Allocation for Storage Services*](https://www.usenix.org/conference/fast26/presentation/ye).

## Hardware Requirements

BunnyFS requires a machine with an NVMe SSD and at least 36 CPU cores.
To better reproduce the results, an Optane SSD is preferred, since the performance modeling is based on Optane.

## Quick Reproduction

Here we describe the procedure for reproducing the experiments on a machine managed by [ADSL](https://research.cs.wisc.edu/adsl/). All commands assume the current working directory is `bunnyfs/`.

On a freshly restarted machine, set up SPDK, which will pin huge pages for the BunnyFS server. This script may fail if the system memory is already heavily fragmented; in that case, reboot the machine first. Skip this step if SPDK has already been set up.

```shell
python3 scripts/driver.py setup_spdk
```

To prepare the files on the SSD for experiments (this only needs to be done once and remains valid after restart; it requires `setup_spdk` first):

```shell
python3 bench/prep_files.py  # prepare files on the SSD
```

---

To run the scaling experiment:

```shell
python3 bench/exp_32apps.py  # may take ~4 minutes
```

This should produce a figure `results/exp_32apps_hare/result_32apps_improve_cdf.pdf` (Figure 11).

---

To run the dynamic experiment:

```shell
python3 bench/exp_dynamic.py  # may take ~6 minutes
```

This should produce a figure `results/exp_dynamic_hare/result_dynamic_tenants.pdf` (Figure 12).


## Related Configuration Explained

Here we point to a list of configurations that need to be updated when running on non-ADSL machines.

- `scripts/env.py`: this file reads the environment variables `DEV_NAME`, `PCIE_ADDR`, `MNT_PATH`; the default values are set based on the hardware in ADSL.
- `cfs/sched/Param.h`: this file configures hardware CPU frequency (`cycles_per_second`) and the worker's available CPU cycles (`worker_avail_weight`); may need to adjust accordingly when running on a new machine.
