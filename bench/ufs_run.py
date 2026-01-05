import logging
import os
import subprocess
import time
from pathlib import Path

from ufs_build import get_ufs_build_dir
from ufs_cleanup import EXIT_FNAME, READY_FNAME
from utils import run_cmd, check_rc
from typing import List


def prep_configs(ufs_config_fname: str = "/tmp/ufs.config",
                 spdk_config_fname: str = "/tmp/spdk.config"):
    if not os.path.exists(ufs_config_fname):
        config_str = '# Generated config\n' \
            '# config for using FSP\n' \
            'splitPolicyNum = "0";\n' \
            'serverCorePolicyNo = "0";\n' \
            'lb_cgst_ql = "0";\n' \
            'nc_percore_ut = "0";\n' \
            'dirtyFlushRatio = "0.9";\n' \
            'raNumBlock = "16";\n'
        logging.info(f"Prepare uFS configuration:\n{config_str}")
        with open(ufs_config_fname, 'w') as f:
            f.write(config_str)

    if not os.path.exists(spdk_config_fname):
        config_str = '# Generated config for SPDK\n' \
            'dev_name = "spdkSSD";\n' \
            'core_mask = "0x2";\n' \
            'shm_id = "9";\n'
        logging.info(f"Prepare SPDK configuration:\n{config_str}")
        with open(spdk_config_fname, 'w') as f:
            f.write(config_str)


def get_ufs_cmd(
    *,
    num_workers: int,
    num_apps: int,
    total_cache_mb: int,
    total_bandwidth_mbps: int,
    core_ids: List[int],  # core id is 1-based
    disable_alloc=False,
    disable_harvest=False,
    disable_avoid_tiny_weight=False,
    disable_cache_partition=False,
    is_symm=True,
):
    assert len(core_ids) == num_workers
    assert is_symm or num_workers % num_apps == 0

    core_ids = ','.join(str(i) for i in core_ids)
    cache_mb_per_app = total_cache_mb / num_apps
    bandwidth_mbps_per_app = total_bandwidth_mbps / num_apps

    if is_symm:
        cpu_share_per_worker = 1 / num_apps
        cache_mb_per_app_per_worker = cache_mb_per_app / num_workers
        bandwidth_mbps_per_app_per_worker = bandwidth_mbps_per_app / num_workers

        cfg = ','.join(
            f"w{wid}-a{aid}:c{int(cache_mb_per_app_per_worker)}"
            f":b{int(bandwidth_mbps_per_app_per_worker)}:p{cpu_share_per_worker}"
            for aid in range(num_apps)
            for wid in range(num_workers)
        )

    else:
        assert num_workers % num_apps == 0
        num_workers_per_app = num_workers // num_apps

        cache_mb_per_app_per_worker = cache_mb_per_app / num_workers_per_app
        bandwidth_mbps_per_app_per_worker = bandwidth_mbps_per_app / num_workers_per_app

        cfg = ','.join(
            f"w{wid}-a{aid}:c{int(cache_mb_per_app_per_worker)}"
            f":b{int(bandwidth_mbps_per_app_per_worker)}:p1"
            for aid in range(num_apps)
            for wid in range(aid * num_workers_per_app, (aid + 1) * num_workers_per_app)
        )
    ufs_path = get_ufs_build_dir() / "fsMain"
    cmd = f"sudo {ufs_path} -w {num_workers} -c {core_ids} -a {num_apps} -l {cfg}"
    policy_flags = []
    if disable_alloc:
        policy_flags.append("NO_ALLOC")
    if disable_harvest:
        policy_flags.append("NO_HARVEST")
    if disable_avoid_tiny_weight:
        policy_flags.append("NO_AVOID_TINY_WEIGHT")
    if not is_symm:
        policy_flags.append("NO_SYMM_PARTITION")
    if disable_cache_partition:
        policy_flags.append("NO_CACHE_PARTITION")
    if policy_flags:
        cmd += f" -p {','.join(policy_flags)}"
    return cmd


def set_cpu_freq(freq: int = 2_900_000):
    paths = [
        p for p in Path("/sys/devices/system/cpu/").glob("cpu*/cpufreq/scaling_max_freq")
        if open(p, "r").readline().strip() != str(freq)
    ]
    if len(paths) == 0:
        logging.info(f"CPU frequency already set to {freq / 1e6:.2f} GHz")
        return

    run_cmd(
        f"sudo sh -c 'echo {freq} | tee {' '.join(str(p) for p in paths)}'",
        ok_msg=f"Set CPU frequency to {freq / 1e6:.2f} GHz",
    )


def run_ufs(cmd: str, output_dir: str = None) -> subprocess.Popen:
    # perform cleanup and then run uFS
    from ufs_cleanup import ufs_cleanup

    set_cpu_freq()
    ufs_cleanup()
    prep_configs()

    if output_dir is not None:
        Path(output_dir).mkdir(parents=True, exist_ok=True)
        cmd = f"{cmd} | tee {output_dir}/ufs.log"
    logging.info(f"Run command: \x1b[34m{cmd}\x1b[0m")

    ufs_proc = subprocess.Popen(cmd, shell=True)

    # spin until uFS servers are ready
    while not os.path.exists(READY_FNAME):
        time.sleep(1e-8)
        if ufs_proc.poll() is not None:
            raise RuntimeError("uFS exits unexpectedly!")
    logging.info("=== uFS is ready ===")
    return ufs_proc


def stop_ufs(ufs_proc: subprocess.Popen) -> None:
    assert ufs_proc is not None
    if ufs_proc.poll() is not None:
        raise RuntimeError("uFS exits before being stopped!")
    logging.info("=== Sending SIGINT to uFS ===")
    run_cmd("sudo killall -2 fsMain")
    try:
        ret = ufs_proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        logging.warning("=== uFS does not exit in 5s; will kill it ===")
        run_cmd("sudo killall -9 fsMain")
        ret = ufs_proc.wait()
    check_rc(ret,
             err_msg=f"uFS does not exit gracefully! exit_code={ret}",
             err_panic=False)
    run_cmd(f'sudo rm -f {EXIT_FNAME}', silent=True, err_panic=False)
