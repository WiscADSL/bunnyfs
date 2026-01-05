#pragma once

#include <spdlog/fmt/bundled/chrono.h>
#include <spdlog/spdlog.h>

#include <csignal>
#include <vector>

#include "fsapi.h"

static std::vector<int> get_worker_keys(int num_workers, int aid) {
  // See cfs/include/param.h
  constexpr static int SHM_KEY_SUBSPACE_SIZE = 1000;

  std::vector<int> worker_keys;
  worker_keys.reserve(num_workers);
  for (int i = 0; i < num_workers; ++i) {
    worker_keys.emplace_back(1 + i * SHM_KEY_SUBSPACE_SIZE + aid);
  }
  return worker_keys;
}

struct UFSContext {
  static constexpr int signals[] = {SIGINT, SIGTERM, SIGSEGV,
                                    SIGFPE, SIGABRT, SIGBUS};
  static constexpr int FS_SHM_KEY_BASE = 20190301;
  inline static bool is_initialized = false;

  explicit UFSContext(const std::vector<int>& worker_keys = {1}) {
    if (is_initialized) {
      SPDLOG_ERROR("UFSContext is already initialized");
      throw std::runtime_error("UFSContext is already initialized");
    }
    // Build comma-separated list of keys for logging
    std::string keys_str;
    for (size_t i = 0; i < worker_keys.size(); ++i) {
      if (i > 0) keys_str += ", ";
      keys_str += std::to_string(worker_keys[i]);
    }
    SPDLOG_INFO("Connecting to FSP w/ keys: [{}]", keys_str);
    std::vector<key_t> keys = worker_keys;
    for (auto& key : keys) key += FS_SHM_KEY_BASE;
    if (auto rc = fs_init_multi(keys.size(), keys.data()); rc != 0) {
      throw std::runtime_error("fs_init failed");
    }
    fs_init_thread_local_mem();
    for (auto sig : signals) signal(sig, signal_handler);
    SPDLOG_INFO("Connected to FSP");
    is_initialized = true;
  }

  UFSContext(int num_workers, int aid)
      : UFSContext(get_worker_keys(num_workers, aid)) {}

  ~UFSContext() {
    for (auto sig : signals) signal(sig, SIG_DFL);
    disconnect();
  }

  static void disconnect() {
    std::thread([]() { timeout_exit(); }).detach();
    SPDLOG_INFO("Disconnecting from FSP...");
    fs_exit();
    fs_cleanup();
    SPDLOG_INFO("Disconnected from FSP");
  }

  static void signal_handler(int signum) {
    int pid = getpid();
    SPDLOG_ERROR("Process {} received signal \"{}\". Exiting...", pid,
                 strsignal(signum));
    disconnect();
    exit(1);
  }

  static void timeout_exit() {
    auto timeout = std::chrono::milliseconds(100);
    std::this_thread::sleep_for(timeout);
    SPDLOG_ERROR("Process {} did not exit after {}. Killing...", getpid(),
                 timeout);
    exit(1);
  }
};
