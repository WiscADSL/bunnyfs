import os
import subprocess
import time
import logging
from typing import List, Tuple, Dict, Optional
import fire
from env import *


def check_rc(ret: int,
             *,
             ok_msg: str = None,
             err_msg: str = None,
             err_panic: bool = True):
    # check whether the return code is zero
    if ret == 0:
        if ok_msg is not None:
            logging.info(ok_msg)
    else:
        if err_panic:
            raise RuntimeError(err_msg)
        if err_msg is not None:
            logging.warning(err_msg)


def run_cmd(cmd: str,
            *,
            ok_msg: str = None,
            err_msg: str = None,
            err_panic: bool = True,
            silent: bool = False):
    # run a command and check return code
    logging.info(f"Run command: `{cmd}`")
    ret = subprocess.call(cmd,
                          shell=True,
                          stdout=subprocess.DEVNULL if silent else None,
                          stderr=subprocess.DEVNULL if silent else None)
    check_rc(ret, err_msg=err_msg, ok_msg=ok_msg, err_panic=err_panic)


def get_numa_node_num() -> int:
    # This way to get NUMA number is not very clean... `lscpu` is really for
    # human, not for machine, but it works on all the machines we used...
    # Maybe update it in the future
    check_numa_out = subprocess.check_output(
        "lscpu | grep -i NUMA", shell=True,
        encoding='utf-8').strip().split("\n")
    # first line should be like "NUMA node(s):         2"
    # rest of lines should be mapping of each NUMA nodes to core ids
    numa_node_num = int(check_numa_out[0].split()[-1])
    assert numa_node_num == len(check_numa_out) - 1
    return numa_node_num


def mkdir(path: str) -> None:
    run_cmd(f"mkdir -p {path}", err_msg=f"Fail to mkdir: {path}")


def rm(path: str, err_panic: bool = False) -> None:
    assert path != "/"  # for obvious reasons
    run_cmd(f"sudo rm -rf {path}", err_panic=err_panic)


def drop_page_cache() -> None:
    run_cmd("sudo sh -c 'sync; echo 1 > /proc/sys/vm/drop_caches'")
    run_cmd("sudo sh -c 'sync; echo 2 > /proc/sys/vm/drop_caches'")
    run_cmd("sudo sh -c 'sync; echo 3 > /proc/sys/vm/drop_caches'")


def cleanup(skip_fs: bool = False) -> None:
    if not skip_fs:
        run_cmd('sudo killall -9 fsMain', silent=True, err_panic=False)
        run_cmd('sudo rm -f /ufs-*', silent=True, err_panic=False)
        run_cmd('sudo rm -f ./compressedLog', silent=True, err_panic=False)
        run_cmd('sudo rm -f /dev/shm/*', silent=True, err_panic=False)
    run_cmd('sudo killall -9 bench', silent=True, err_panic=False)
    run_cmd('sudo killall -9 prep', silent=True, err_panic=False)
    run_cmd('sudo killall -9 tree', silent=True, err_panic=False)
    run_cmd('sudo killall -9 migration_test', silent=True, err_panic=False)
    run_cmd('sudo killall -9 tests', silent=True, err_panic=False)
    run_cmd('sudo killall cfs_bench_coordinator', silent=True, err_panic=False)
    run_cmd('sudo rm /dev/shm/coordinator', silent=True, err_panic=False)
    drop_page_cache()


def setup_spdk(numa_node_num=None) -> None:
    if not os.path.exists(DEV_PATH):  # make sure DEV_PATH exists
        logging.warning(f"Device {DEV_PATH} does not exist; it can happen if SPDK has been set up already.")
        return

    if numa_node_num is None:
        numa_node_num = 1  # only pin NUMA node 0
    numa_node_list = range(numa_node_num)
    for no in numa_node_list:
        run_cmd(
            f'HUGEMEM=16384 PCI_WHITELIST="{PCIE_ADDR}" HUGENODE={no} '
            f'sudo -E {PROJ_PATH}/cfs/lib/spdk/scripts/setup.sh',
            err_msg=f"Fail to pin mem into node {no}!")
    ulimit_out = subprocess.check_output("sudo -E bash -c 'ulimit -l'",
                                         shell=True,
                                         encoding='utf-8').strip()
    if ulimit_out != "unlimited":
        logging.warning("ulimit -l is not unlimited, "
                        "you may not be able to run SPDK, "
                        "please check `/etc/security/limits.conf`")
    run_cmd('echo 0 | sudo tee /proc/sys/kernel/randomize_va_space',
            err_msg="Fail to disable ASLR!",
            silent=True)
    logging.info('DONE: Setup SPDK')


def reset_spdk() -> None:
    run_cmd(f'sudo -E {PROJ_PATH}/cfs/lib/spdk/scripts/setup.sh reset',
            ok_msg='DONE: Reset SPDK',
            err_msg='Fail to reset SPDK')


def setup_ext4(has_journal=True,
               readahead_kb=None,
               delay_allocate=True) -> None:
    # mkfs
    run_cmd(f"sudo mkfs -F -t ext4 {DEV_PATH}", err_msg="Fail to mkfs!")

    # mount
    run_cmd(f'sudo mkdir -p {MNT_PATH}', err_msg=f"Fail to mkdir {MNT_PATH}!")
    if not has_journal:
        run_cmd(f'sudo tune2fs -O ^has_journal {DEV_PATH}',
                err_msg="Fail to disable journal!")

    run_cmd(
        f'sudo mount {"" if delay_allocate else "-o nodelalloc"} '
        f'{DEV_PATH} {MNT_PATH}',
        err_msg=f"Fail to mount to {MNT_PATH}")

    # set readahead
    if readahead_kb is not None:
        run_cmd(f'sudo blockdev --setra {readahead_kb} {DEV_PATH}',
                ok_msg=f"Set readahead to {readahead_kb} KB",
                err_msg="Fail to set readahead!")

    # create bench data dir
    run_cmd(f'sudo mkdir -p {MNT_PATH}/bench',
            err_msg="Fail to create bench directory")
    logging.info('DONE: Setup ext4')


def reset_ext4() -> None:
    run_cmd(f'sudo umount {DEV_PATH}', err_msg="Fail to umount ext4!")
    run_cmd(f'sudo blockdev --setra 128 {DEV_PATH}',
            err_msg="Fail to reset readahead")
    logging.info('DONE: Reset ext4')


def ufs_mkfs(log_dir: Optional[str] = None) -> None:
    run_cmd(
        f'sudo -E {PROJ_PATH}/cfs/build/test/fsproc/testRWFsUtil mkfs'
        f' {f"> {log_dir}/mkfs.log" if log_dir is not None else ""}',
        ok_msg="DONE: uFS mkfs",
        err_msg="Fail to mkfs for uFS!")


def ufs_ckpt(log_dir: Optional[str] = None) -> None:
    run_cmd(
        f'sudo -E {PROJ_PATH}/cfs/build/test/fsproc/fsProcOfflineCheckpointer'
        f' {f"> {log_dir}/ckpt.log" if log_dir is not None else ""}',
        ok_msg="DONE: uFS checkpointing",
        err_msg="Fail to checkpoint for uFS!")


EXIT_FNAME = "/tmp/ufs_exit"
READY_FNAME = "/tmp/ufs_ready"


def get_shm_names(num_workers: int, num_apps: int) -> Tuple[str, str]:
    # key_subspace_size is a magic number hardcoded in cfs/include/param.h
    # worker 0 gets key space 1-1000, worker 1 gets key space 1001-2000, etc
    # unfortunately, as a legacy style from the System V's shared memory
    # subsystem, key_t is an int, so we have to use such a dirty way to encode
    # wid and aid into an int
    key_subspace_size = 10
    app_shm_name_map = {
        aid: ",".join([
            f"{(1 + i * key_subspace_size + aid)}" for i in range(num_workers)
        ])
        for aid in range(num_apps)
    }
    return app_shm_name_map


def prep_configs(ufs_config_fname: str, spdk_config_fname: str):

    if not os.path.exists(ufs_config_fname):
        config_str = '# Generated config\n' \
            '# config for using FSP\n'\
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
        config_str = '# Generated config for SPDK\ndev_name = "spdkSSD";\n' \
            f'core_mask = "0x2";\nshm_id = "9";\n'
        logging.info(f"Prepare SPDK configuration:\n{config_str}")
        with open(spdk_config_fname, 'w') as f:
            f.write(config_str)


def start_ufs(num_apps: int,
              worker_cores: List[int],
              worker_app_configs: List[str],
              log_dir: str,
              ufs_config_fname: str = "/tmp/ufs.config",
              spdk_config_fname: str = "/tmp/spdk.config",
              skip_fs: bool = False) -> Tuple[subprocess.Popen, Dict]:
    log_fname = f"{log_dir}/ufs.log"
    num_workers = len(worker_cores)
    rm(READY_FNAME)
    rm(EXIT_FNAME)

    if skip_fs:
        return None

    prep_configs(ufs_config_fname=ufs_config_fname,
                 spdk_config_fname=spdk_config_fname)

    fs_cmd = f"sudo -E {PROJ_PATH}/cfs/build/fsMain " \
        f"-w {num_workers} -a {num_apps} " \
        f"-c {','.join(map(str, worker_cores))} "\
        f"-l {','.join(worker_app_configs)} " \
        f"-r {READY_FNAME} -e {EXIT_FNAME} "\
        f"-f {ufs_config_fname} -d {spdk_config_fname} " \

    with open(log_fname, "w") as f:
        f.write("=== Command ===\n")
        f.write(fs_cmd)
        f.write("\n===============\n")

    # we run checkpoint first
    ufs_ckpt(log_dir)

    cmd = f"{fs_cmd} | tee -a {log_fname}"
    logging.info(f"Running cmd `{cmd}`")
    ufs_proc = subprocess.Popen(cmd, shell=True)

    # spin until uFS servers are ready
    while not os.path.exists(READY_FNAME):
        time.sleep(1e-8)
        if ufs_proc.poll() is not None:
            raise RuntimeError("uFS exits unexpectedly!")
    return ufs_proc


def stop_ufs(ufs_proc: subprocess.Popen) -> None:
    if not ufs_proc:  # if None, do nothing
        return
    if ufs_proc.poll() is not None:
        raise RuntimeError("uFS exits before being stopped!")
    with open(EXIT_FNAME, "w") as f:
        f.write('Apparate')
        f.flush()
    ret = ufs_proc.wait()
    check_rc(ret, err_msg="uFS does not exit gracefully!", err_panic=False)
    rm(READY_FNAME)
    rm(EXIT_FNAME)


def start_bench_coord(num_threads: int, log_dir: str) -> subprocess.Popen:
    cmd = (f"{PROJ_PATH}/cfs_bench/build/bins/cfs_bench_coordinator "
           f"-n {num_threads} 2>&1 > {log_dir}/coord.log")
    logging.info(f"Running cmd `{cmd}`")
    return subprocess.Popen(cmd, shell=True)


def decmpr_nanolog():
    esc_path = str(PROJ_PATH).replace("/", "\/")
    sed_cmd = f"sed 's/{esc_path}\/cfs\/sched\///' | sed 's/{esc_path}\/cfs\///'"
    run_cmd(
        f"{PROJ_PATH}/cfs/lib/Nanolog/runtime/decompressor decompress "
        f"./compressedLog | {sed_cmd} > ./ufs.nanolog",
        err_msg="Fail to decompress Nanolog!",
        err_panic=True)


class uFsDriver:

    def __init__(self) -> None:
        pass

    def start(self,
              num_apps: int,
              worker_cores: List[int],
              worker_app_configs: List[str],
              log_dir: str,
              skip_fs: bool = False) -> None:
        self.ufs_proc = start_ufs(num_apps=num_apps,
                                  worker_cores=worker_cores,
                                  worker_app_configs=worker_app_configs,
                                  log_dir=log_dir,
                                  skip_fs=skip_fs)
        self.app_shm_name_map = get_shm_names(num_workers=len(worker_cores),
                                              num_apps=num_apps)

    def stop(self) -> None:
        stop_ufs(self.ufs_proc)

    def get_app_shm_names(self, aid) -> str:
        return self.app_shm_name_map[aid]


class FireTrigger:
    """Wrapper to use fire module"""

    def drop_page_cache(self):
        drop_page_cache()

    def cleanup(self, skip_fs: bool = False) -> None:
        cleanup(skip_fs)

    def setup_spdk(self, numa_node_num=None) -> None:
        setup_spdk(numa_node_num)

    def reset_spdk(self) -> None:
        reset_spdk()

    def setup_ext4(self) -> None:
        setup_ext4()

    def reset_ext4(self) -> None:
        reset_ext4()

    def prep_configs(self,
                     ufs_config_fname: str = "/tmp/ufs.config",
                     spdk_config_fname: str = "/tmp/spdk.config") -> None:
        prep_configs(ufs_config_fname=ufs_config_fname,
                     spdk_config_fname=spdk_config_fname)

    def ufs_mkfs(self, log_dir: Optional[str] = None) -> None:
        ufs_mkfs(log_dir)

    def ufs_ckpt(self, log_dir: Optional[str] = None) -> None:
        ufs_ckpt(log_dir)

    def start_bench_coord(self, num_threads: int, log_dir: str) -> None:
        start_bench_coord(num_threads, log_dir)

    def decmpr_nanolog(self) -> None:
        decmpr_nanolog()


if __name__ == '__main__':
    fire.Fire(FireTrigger)
