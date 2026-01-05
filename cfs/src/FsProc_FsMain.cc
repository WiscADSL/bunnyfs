#include <stdio.h>
#include <sys/types.h>
#include <unistd.h>

#include <experimental/filesystem>
#include <iostream>
#include <memory>

#include "FsLibShared.h"
#include "FsProc_Fs.h"
#include "Log.h"
#include "Param.h"
#include "config4cpp/Configuration.h"
#include "perfutil/Stats.h"
#include "spdlog/spdlog.h"
#include "typedefs.h"

// global fs object
FsProc* gFsProcPtr = nullptr;

void handle_sigint(int sig);

int fsMain(int numWorkers, int numAppProc, std::vector<int>& workerCores,
           const char* readySignalFileName, const char* exitSignalFileName,
           const char* uFSConfigFileName, const char* SPDKConfigFileName,
           std::vector<std::vector<std::tuple<int, int, double, double>>>&
               workerAppConfigs,
           bool isSpdk = true) {
  macro_print(NMEM_DATA_BLOCK);
  std::vector<CurBlkDev*> devVec;
  // NOTE: devPtr is used as a global variable previously
  CurBlkDev* devPtr = nullptr;
  if (isSpdk)
    devPtr = new CurBlkDev("", DEV_SIZE / BSIZE, BSIZE, SPDKConfigFileName);
  else
    devPtr = new CurBlkDev(BLK_DEV_POSIX_FILE_NAME, DEV_SIZE / BSIZE, BSIZE,
                           SPDKConfigFileName);
  devPtr->updateWorkerNum(numWorkers);
  // For both BlkDevSpdk and BlkDevPosix, we let all threads share the same
  // virtual block device
  for (int i = 0; i < numWorkers; i++) devVec.push_back(devPtr);
  gFsProcPtr = new FsProc(numWorkers, numAppProc, readySignalFileName,
                          exitSignalFileName);
  gFsProcPtr->setConfigFname(uFSConfigFileName);

#ifdef FSP_ENABLE_ALLOC_READ_RA
  std::cout << "READAHEAD raNumBlocks:" << gFsProcPtr->getRaNumBlock()
            << std::endl;
#endif

  std::cout << "ServerCorePolicy:" << gFsProcPtr->getServerCorePolicyNo()
            << std::endl;
  std::cout << "lb_cgst_ql:" << gFsProcPtr->GetLbCgstQl() << std::endl;
  std::cout << "nc_percore_ut:" << gFsProcPtr->GetNcPerCoreUt() << std::endl;

  signal(SIGINT, handle_sigint);
  // start workers
  // for now, we only use static offsets instead of inputting for cmd-line
  std::vector<int> shmBaseOffsets;
  for (int wid = 0; wid < numWorkers; ++wid)
    shmBaseOffsets.emplace_back(wid * SHM_KEY_SUBSPACE_SIZE + 1);
  // 1000 is a magic number; must match with microbench scripts
  // the only goal is to make sure workers having disjointed shm name space
  gFsProcPtr->startWorkers(shmBaseOffsets, workerAppConfigs, devVec,
                           workerCores);
  return 0;
}

// see usage message below
void usage(char** argv) {
  std::cerr << "Usage:\n"
            << argv[0]
            << " -w NUM_WORKERS -a NUM_APPS -c CORE_LIST -l CONFIG_LIST\n"
            << "  [-r READY_FILENAME] [-e EXIT_FILENAME] "
               "[-f UFS_CONFIG] [-d SPDK_CONFIG] [-p POLICY]\n\n";
  std::cerr
      << "  -w NUM_WORKERS      number of workers to create\n"
      << "  -a NUM_APPS         number of apps that will attach\n"
      << "  -c CORE_LIST        a comma-separated list of cores to pin\n"
      << "                      workers; length must match NUM_WORKERS\n"
      << "  -l CONFIG_LIST      a comma-separated list, where each element\n"
      << "                      must be formatted as \"wX-aY:cZ:bW:pV\" where\n"
      << "                      X is worker id, Y is an app id, Z is the\n"
      << "                      initial cache size (in MB) for app Y in \n"
      << "                      worker X, W is the I/O bandwidth in MB/s),\n"
      << "                      V is the CPU ratio on the worker,"
      << "                      correspondingly\n"
      << "  -r READY_FILENAME   name of ready signal file, which is created\n"
      << "                      by uFS to indicate it is ready\n"
      << "  -e EXIT_FILENAME    name of exit signal file, which asks uFS to\n"
      << "                      shutdown\n"
      << "  -f UFS_CONFIG       path to uFS config file (`f' for filesystem)\n"
      << "  -d SPDK_CONFIG      path to SPDK config file (`d' for device)\n"
      << "  -p POLICY           policy flags as a comma-separated string\n";
}

void check_root() {
  int uid = getuid();
  if (uid != 0) {
    printOnErrorExitSymbol();
    std::cerr << "Error, must be invoked in root mode. \nExit ......\n";
    exit(1);
  }
}

void check_cpu_freq() {
  // verify CPU frequency matches the params
  // NOTE: rdtsc frequency differs from the real CPU frequency!
  // check `lscpu | grep 'Model name'` to see (e.g., xxx CPU @ 2.10GHz)
  uint64_t t0 = PlatformLab::PerfUtils::Cycles::rdtsc();
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  int64_t t = PlatformLab::PerfUtils::Cycles::rdtsc() - t0;
  int64_t t_diff = t * 10 - (sched::params::cycles_per_second);

  if (t_diff < sched::params::cycles_per_second * -0.05 ||
      t_diff > sched::params::cycles_per_second * 0.05) {
    std::cerr << "Measured frequency: " << t * 10
              << "; Expect frequency: " << sched::params::cycles_per_second
              << std::endl;
    throw std::runtime_error("Incorrect CPU frequency (error > 5%)");
  }
}

void logFeatureMacros() {
#if CFS_JOURNAL(NO_JOURNAL)
  SPDLOG_INFO("CFS_JOURNAL(NO_JOURNAL) = True");
#endif

#if CFS_JOURNAL(ON)
  SPDLOG_INFO("CFS_JOURNAL(ON) = True");
#endif

#if CFS_JOURNAL(LOCAL_JOURNAL)
  SPDLOG_INFO("CFS_JOURNAL(LOCAL_JOURNAL) = True");
#endif

#if CFS_JOURNAL(GLOBAL_JOURNAL)
  SPDLOG_INFO("CFS_JOURNAL(GLOBAL_JOURNAL) = True");
#endif

#if CFS_JOURNAL(PERF_METRICS)
  SPDLOG_INFO("CFS_JOURNAL(PERF_METRICS) = True");
#endif

#if !CFS_JOURNAL(CHECKPOINTING)
  // This should only be used when testing writes where you don't want
  // checkpointing to be measured. It will fail offlineCheckpointer so it cannot
  // be used accross multiple runs of fsp. Mkfs must be called after this run of
  // fsp.
  SPDLOG_WARN("CFS_JOURNAL(CHECKPOINTING) = False");
#endif
}

static const int gHostNameLen = 512;
static char gHostName[gHostNameLen];

int main(int argc, char** argv) {
  check_root();
  check_cpu_freq();

  google::InitGoogleLogging(argv[0]);

  // print hostname
  gethostname(gHostName, gHostNameLen);
  std::cout << argv[0] << " started in host:" << gHostName << "\n";

#ifndef USE_SPDK
  std::cerr << "SPDK is now required but USE_SPDK is not defined!\n";
  abort();
#endif

#ifndef NONE_MT_LOCK
  std::cout << "NONE_MT_LOC - OFF" << std::endl;
#else
  std::cout << "NONE_MT_LOC - ON" << std::endl;
#endif

#ifndef MIMIC_FSP_ZC
  std::cout << "MIMIC_FSP_ZC - OFF" << std::endl;
#else
  std::cout << "MIMIC_FSP_ZC - ON" << std::endl;
#endif

#if (FS_LIB_USE_APP_CACHE)
  std::cout << "FS_LIB_USE_APP_CACHE - ON" << std::endl;
#else
  std::cout << "FS_LIB_USE_APP_CACHE - OFF" << std::endl;
#endif

#ifdef FSP_ENABLE_ALLOC_READ_RA
  std::cout << "FS_ENABLE_ALLOC_READ_RA - ON" << std::endl;
#else
  std::cout << "FS_ENABLE_ALLOC_READ_RA - OFF" << std::endl;
#endif

#if CFS_JOURNAL(OFF)
  std::cout << "Journal is disabled\n";
#else
  std::cout << "Journal is enabled\n";
#endif
#ifdef NDEBUG
  std::cout << "NDEBUG defined\n";
#else
  std::cout << "NDEBUG not defined\n";
#endif

  logFeatureMacros();

  opterr = 0;
  int c;
  int num_workers = 0;
  int num_apps = 0;
  std::vector<int> worker_cores;
  const char* ready_filename = DEFAULT_READY_FILENAME;
  const char* exit_filename = DEFAULT_EXIT_FILENAME;
  const char* ufs_config = DEFAULT_UFS_CONFIG;
  const char* spdk_config = DEFAULT_SPDK_CONFIG;
  // each element corresponds to a worker's list, which contains all apps that
  // would reach out and their associated initial cache size and bandwidth
  // each config is tuple <aid, cache_mb, bw_mb>
  std::vector<std::vector<std::tuple<int, int, double, double>>>
      worker_app_configs;

#define check_file_exists(filename)                         \
  do {                                                      \
    if (!std::experimental::filesystem::exists(filename)) { \
      std::cerr << "Error: " #filename "=" << (filename)    \
                << " already exists!\n";                    \
      goto err;                                             \
    }                                                       \
  } while (0);

#define check_file_not_exists(filename)                    \
  do {                                                     \
    if (std::experimental::filesystem::exists(filename)) { \
      std::cerr << "Error: " #filename "=" << (filename)   \
                << " does not exist!\n";                   \
      goto err;                                            \
    }                                                      \
  } while (0);

  while ((c = getopt(argc, argv, "w:a:c:l:r:e:f:d:p:")) != -1) {
    switch (c) {
      case 'w':
        num_workers = atoi(optarg);
        worker_app_configs.clear();
        worker_app_configs.resize(num_workers);
        break;
      case 'a':
        num_apps = atoi(optarg);
        break;
      case 'c':
        worker_cores.clear();
        for (auto s : splitStr(std::string(optarg), ','))
          worker_cores.emplace_back(std::stoi(s));
        // this may raise an exception for invalid arguments
        break;
      case 'r':
        ready_filename = optarg;
        check_file_not_exists(ready_filename);
        break;
      case 'e':
        exit_filename = optarg;
        check_file_not_exists(exit_filename);
        break;
      case 'f':
        ufs_config = optarg;
        check_file_exists(ufs_config);
        break;
      case 'd':
        spdk_config = optarg;
        check_file_exists(spdk_config);
        break;
      case 'l':
        for (auto s : splitStr(std::string(optarg), ',')) {
          int w, a, cache_mb;
          double bandwidth_mb, cpu_ratio;
          if (sscanf(s.data(), "w%d-a%d:c%d:b%lf:p%lf", &w, &a, &cache_mb,
                     &bandwidth_mb, &cpu_ratio) != 5) {
            std::cerr << "Invalid configuration: " << s << '\n';
            goto err;
            if (cpu_ratio > 1) {
              std::cerr << "Invalid configuration: CPU ratio must be <= 1" << s
                        << '\n';
              goto err;
            }
          }
          if (w >= num_workers) {
            std::cerr << "Worker " << w << " does not exist!\n";
            goto err;
          }
          if (a >= num_apps) {
            std::cerr << "App " << a << " does not exist!\n";
            goto err;
          }
          worker_app_configs[w].emplace_back(a, cache_mb, bandwidth_mb,
                                             cpu_ratio);
        }
        break;
      case 'p':
        for (auto s : splitStr(std::string(optarg), ',')) {
          if (s == "NO_ALLOC") {
            sched::params::policy::alloc_enabled = false;
          } else if (s == "NO_HARVEST") {
            sched::params::policy::harvest_enabled = false;
          } else if (s == "NO_SYMM_PARTITION") {
            sched::params::policy::symm_partition = false;
          } else if (s == "NO_AVOID_TINY_WEIGHT") {
            sched::params::policy::avoid_tiny_weight = false;
          } else if (s == "NO_CACHE_PARTITION") {
            sched::params::policy::cache_partition = false;
          } else {
            std::cerr << "Unknown policy flag: " << optarg << std::endl;
            goto err;
          }
        }
        break;
      case '?':
        std::cerr << "Unknown option `-" << char(optopt) << "'.\n";
      default:
        goto err;
    }
  }
#undef check_file_exists
#undef check_file_not_exists
  SPDLOG_INFO(
      "fsMain with num_workers={}, num_apps={}, "
      "worker_cores={}, ready_filename={}, exit_filename={}, "
      "ufs_config={}, spdk_config={}",
      num_workers, num_apps, fmt::join(worker_cores, ","), ready_filename,
      exit_filename, ufs_config, spdk_config);
  if (num_workers <= 0) {
    std::cerr << "No valid <num_workers> specified!\n";
    goto err;
  }
  if (num_apps <= 0) {
    std::cerr << "No valid <num_apps> specified!\n";
    goto err;
  }
  if (static_cast<int>(worker_cores.size()) != num_workers) {
    std::cerr << "<num_workers> and <core_list> mismatch!\n";
    goto err;
  }
  if (worker_app_configs.size() == 0) {
    std::cerr << "[WARN] No valid worker-app configuration specified; none "
                 "of apps' resources are not limited\n";
    // for now, we consider this is a legal behavior e.g. run a no-scheduling
    // workload or some basic R/W cmdline tool
  }

  // print policy flags
  sched::params::log_params();

  sched::log::init();

  SCHED_LOG_NOTICE("NANOLOG IS RUNNING... ");

  fsMain(num_workers, num_apps, worker_cores, ready_filename, exit_filename,
         ufs_config, spdk_config, worker_app_configs);

  sched::log::destroy();

  return 0;

err:
  usage(argv);
  return 1;
}

void handle_sigint(int sig) {
  gFsProcPtr->stop();
  // delete gFsProcPtr;
  // exit(0);
}
