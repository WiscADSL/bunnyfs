#pragma once

#include <fcntl.h>
#include <unistd.h>

#include <stdexcept>

/**
 * Pin the current thread to the given core.
 * @param core_id 1-based core id.
 */
static void pin_to_core(int core_id) {
  if (core_id < 0) return;
  cpu_set_t cpuset;
  CPU_ZERO(&cpuset);
  CPU_SET(core_id - 1, &cpuset);  // sched_setaffinity takes 0-based core id
  int rc = sched_setaffinity(0, sizeof(cpu_set_t), &cpuset);
  if (rc != 0) {
    throw std::runtime_error("sched_setaffinity failed");
  }
}
