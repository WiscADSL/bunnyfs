#pragma once

#include <fcntl.h>
#include <leveldb/db.h>
#include <spdlog/fmt/ranges.h>
#include <spdlog/spdlog.h>

#include <random>
#include <thread>

#include "args.h"
#include "config.h"
#include "fsapi.h"
#include "spec.h"
#include "utils/barrier.h"
#include "utils/leveldb.h"
#include "utils/logging.h"
#include "utils/pin.h"
#include "utils/ufs.h"
#include "workload.h"

struct Thread {
  static void main(const spec::Thread& thread, const Barrier& barrier) {
    init_thread_local_logger(thread.name, thread.log_path);

    THREAD_INFO("Thread \"{}\" started at core {}", thread.name, thread.core);
    pin_to_core(thread.core);
    fs_init_thread_local_mem();
    assign_worker(thread.worker_id, thread.name);

    if (thread.type == spec::ThreadType::RW) {
      // open thread-local files and wait for all threads to finish
      int curr_owner =
          thread.worker_id >= 0 ? thread.worker_id : /*primary wid*/ 0;
      std::vector<int> fds =
          open_files(thread.file_paths, thread.pin_file_map, curr_owner);
      barrier.arrive_and_wait();

      // shuffle fds: run_workload will access fd in order; to avoid lock-step
      // access and bursty load to one file, we want each thread to read
      // different files
      std::shuffle(fds.begin(), fds.end(),
                   std::default_random_engine(std::random_device()()));

      // wait for all threads to finish populating fds
      barrier.arrive_and_wait();

      // run workloads
      for (const auto& workload : thread.workloads) {
        Workload(workload).run(fds);
      }

      // wait for all threads to finish and close files
      barrier.arrive_and_wait();
      for (int fd : fds) fs_close(fd);
    } else if (thread.type == spec::ThreadType::DB) {
      auto db = open_db(thread.db_path);
      db->CompactRange(nullptr, nullptr);
      fs_syncall();

      barrier.arrive_and_wait();

      for (const auto& workload : thread.workloads) {
        Workload(workload).run(db);
      }
      // db->CompactRange(nullptr, nullptr);
      std::this_thread::sleep_for(std::chrono::milliseconds{50});
      delete db;

      // LevelDB modifies the manifest file on open so we need to
      // call fsync to ensure all data is flushed to disk
      fs_syncall();
    } else {
      throw std::runtime_error("Unknown thread type");
    }

    THREAD_INFO("Thread \"{}\" finished", thread.name);
  }

  static std::vector<int> open_files(
      const std::vector<std::string>& file_paths,
      const std::unordered_map<std::string, int>& pin_file_map,
      int curr_owner) {
    std::vector<int> fds;
    int num_pin = 0;
    for (const auto& path : file_paths) {
      THREAD_DEBUG("Opening file \"{}\"...", path);
      int fd = fs_open(path.c_str(), O_RDWR, 0644);
      if (fd < 0) {
        THREAD_ERROR("Failed to open file {}", path);
        exit(1);
      }
      fds.push_back(fd);
      auto it = pin_file_map.find(path);
      if (it != pin_file_map.end()) {
        struct stat stat_buf;
        int fstat_rc = fs_fstat(fd, &stat_buf);
        if (fstat_rc) {
          THREAD_ERROR("Fail to get stat from fd={}", fd);
          exit(1);
        }
        /*
         * fs_admin_inode_reassignment usage:
         * type == 0: Check if curOwner is the owner of the inode. newOwner is
         *     ignored. Returning 0 indicates success - it is the owner.
         * type == 1: Move inode from curOwner to newOwner if curOwner is
         *     really the owner. Returning 0 indicates successful migration.
         */
        assert(fs_admin_inode_reassignment(/*type*/ 0, stat_buf.st_ino,
                                           curr_owner,
                                           /*ignored*/ 0) == 0);
        int pin_rc = fs_admin_inode_reassignment(/*type*/ 1, stat_buf.st_ino,
                                                 curr_owner, it->second);
        if (pin_rc) {
          THREAD_ERROR("Fail to pin fd={} from worker {} to {}", fd, curr_owner,
                       it->second);
          exit(1);
        }
        ++num_pin;
      }
    }
    if (num_pin != pin_file_map.size()) {
      THREAD_ERROR("Number of files pinned does not match pin_file_map!");
      exit(1);
    }
    return fds;
  }

  static void assign_worker(int worker_id,
                            const std::string& name = "Unnamed") {
    if (worker_id == -1) {
      THREAD_WARN("Thread \"{}\" not assigned to a worker", name);
    } else if (worker_id == 0) {
      THREAD_INFO("Thread \"{}\" already assigned to worker {}", name,
                  worker_id);
    } else {
      int rc = fs_admin_thread_reassign(0, worker_id, FS_REASSIGN_ALL);
      if (rc < 0) {
        THREAD_ERROR("Failed to reassign thread \"{}\" to worker {}", name,
                     worker_id);
      } else {
        THREAD_INFO("Thread \"{}\" reassigned to worker {}", name, worker_id);
      }
    }
  }
};
