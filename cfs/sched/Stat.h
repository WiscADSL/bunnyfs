#pragma once

#include <cstdint>
#include <string>

#include "Log.h"
#include "Param.h"
#include "perfutil/Cycles.h"

namespace sched::stat {

class LatencyStat {
  // reports latency every X ops (for now, 2^19 op means every 256 MB IO)
  constexpr static uint64_t report_latency_freq = (1UL << 19);
  constexpr static uint64_t cycles_per_us =
      sched::params::cycles_per_second / 1000000;

  uint64_t latency_sum{0};  // unit: cycles
  uint64_t num_ops{0};

  std::string stat_name;

 public:
  LatencyStat() = default;

  void set_name(const char* name) { stat_name = name; }

  void add_latency(uint64_t l) {
    latency_sum += l;
    ++num_ops;
    if (num_ops >= report_latency_freq) {
      SCHED_LOG_NOTICE("[STAT] %s latency: %.1lf us/op", stat_name.c_str(),
                       double(latency_sum) / cycles_per_us / num_ops);
      latency_sum = 0;
      num_ops = 0;
    }
  }
};

class IdleStat {
  constexpr static uint64_t report_idle_freq_cycles = params::cycles_per_second;

  uint64_t last_report_ts{0};
  uint64_t idle_time_sum{0};
  uint64_t begin_ts{0};
  int wid;

 public:
  IdleStat(int wid) : wid(wid) {}

  // start timer; however, this ts can be ignored if later we find a time window
  // is not idle
  void start() { begin_ts = PlatformLab::PerfUtils::Cycles::rdtsc(); }
  void stop() {
    uint64_t now = PlatformLab::PerfUtils::Cycles::rdtsc();
    uint64_t t_diff = now - begin_ts;
    uint64_t t_since_last = now - last_report_ts;
    if (t_since_last > report_idle_freq_cycles) {
      if (last_report_ts != 0) {  // report idleness
        SCHED_LOG_NOTICE("[STAT] Worker-%d idleness: %.1f%%", wid,
                         100.0 * idle_time_sum / t_since_last);
        idle_time_sum = 0;
      }
      last_report_ts = now;
    } else {
      idle_time_sum += t_diff;
    }
  }
};
}  // namespace sched::stat
