# read environment variables and export them as python global variables
import os
import logging
import subprocess
from pathlib import Path

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_env_var(name, default):
    result = os.environ.get(name)
    if result is not None:
        logger.info(f"Using {name}={result} from environment variable")
    else:
        result = default() if callable(default) else default
        logger.info(f"Using {name}={result} from default value")
    return result


def get_default_proj_path():
    return Path(__file__).absolute().parent.parent


def get_default_dev_name():
    return "nvme0n1"


def get_default_pcie_addr(dev_name: str):
    return "0000:86:00.0"


def lookup_pcie_addr(dev_name: str):
    """
    Here are two ways to lookup for PCIe address:
        - from `/sys/block/{dev_name}/device/address`: this can ensure we are
          looking for the PCIe address of the right SSD, but this won't work
          after SPDK taking over the SSD, in which case the SSD will not show up
          under `/sys/block` anymore
        - from `lspci`: this still works after SPDK taking over the SSD, but we
          cannot match PCIe address to the right SSD
    We pick the first one for now.
    """
    result = subprocess.run(
        f"cat /sys/block/{dev_name}/device/address", shell=True, capture_output=True)
    return result.stdout.decode("utf-8").strip()
    ### Second approach ###
    # result = subprocess.run(
    #     "lspci -D | grep -i -E 'nvme|optane'", shell=True, capture_output=True)
    # lines = result.stdout.decode("utf-8").splitlines()
    # addrs = [line.split()[0] for line in lines]
    # if len(addrs) == 0:
    #     raise ValueError("No NVMe device found")
    # chosen_addr = addrs[0]   # use the first one by default
    # # we have a list of known candidates; if any of them presents, choose it
    # for candi_addr in ["0000:3b:00.0", "0000:5e:00.0"]:
    #     if candi_addr in addrs:
    #         chosen_addr = candi_addr
    #         break
    # logger.info(f"Detect PCIe address: {addrs}; choose {chosen_addr}")
    # return chosen_addr


# not to exposed; just to make sure DEV_PATH and PCIE_ADDR are consistent
DEV_NAME = get_env_var("DEV_NAME", get_default_dev_name)

__all__ = ["PROJ_PATH", "PCIE_ADDR", "DEV_PATH", "MNT_PATH"]
PROJ_PATH = get_env_var("PROJ_PATH", get_default_proj_path)
DEV_PATH = f'/dev/{DEV_NAME}'
PCIE_ADDR = get_env_var("PCIE_ADDR", get_default_pcie_addr(DEV_NAME))
# MNT_PATH only used for ext4; uFS does not have a mount path
MNT_PATH = get_env_var("MNT_PATH", "/ssd-data")


def check_dev_pice_match():
    dev_name = DEV_PATH[5:]
    assert (get_default_pcie_addr(dev_name)) == PCIE_ADDR

### Enable this check if necessary ###
# check_dev_pice_match()
