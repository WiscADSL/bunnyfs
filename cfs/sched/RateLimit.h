#pragma once
#include <algorithm>
#include <atomic>
#include <cstdint>

#include "Param.h"
#include "perfutil/Cycles.h"
#include "spdlog/spdlog.h"

namespace sched {
class RateLimiter {
  std::atomic_uint64_t rate_inv;  // #cycles/block (i.e. inverse of rate)

  // for each time frame, record how many blocks have been sent
  uint64_t curr_time_frame;
  uint64_t curr_num_blks;

  bool is_on = true;

  uint64_t update_time_frame() {
    uint64_t ts = PlatformLab::PerfUtils::Cycles::rdtsc();
    uint64_t tf = ts / params::rate::cycles_per_frame;
    auto time_offset = ts - (tf * params::rate::cycles_per_frame);
    if (tf > curr_time_frame) {
      SPDLOG_DEBUG("rate: target = {} MB/s, actual = {} MB/s",
                   rate_inv_to_bw_mbps(rate_inv),
                   params::blocks_to_mb(curr_num_blks) /
                       (1. * params::rate::cycles_per_frame /
                        params::cycles_per_second));
      curr_time_frame = tf;
      curr_num_blks = 0;
    }
    return time_offset;
  }

  static uint64_t bw_to_rate_inv(int64_t bw) {
    return params::cycles_per_second / std::max(bw, params::min_bandwidth);
  }

  static uint64_t rate_inv_to_bw(uint64_t rate_inv) {
    return params::cycles_per_second / rate_inv;
  }

  static double rate_inv_to_bw_mbps(uint64_t rate_inv) {
    return params::blocks_to_mb(rate_inv_to_bw(rate_inv));
  }

 public:
  // we enforce that when the allocate bandwidth is very low (even zero), we
  // have a minimum guarantee. such minimum bandwidth is not visible to the
  // allocator and thus cannot be used to trade in harvest. this ensures that
  // even when the app's workload changes from no bandwidth demand to need
  // bandwidth, it could still make progress to reflect such workload changes.
  RateLimiter(int64_t bandwidth)
      : rate_inv(bw_to_rate_inv(bandwidth)),
        curr_time_frame(0),
        curr_num_blks(0) {}

  void update_bandwidth(int64_t new_bandwidth) {
    rate_inv.store(bw_to_rate_inv(new_bandwidth), std::memory_order_release);
  }

  // only permit one request at a time
  bool can_send() {
    if (!is_on) return true;  // happy hour: unlimited bandwidth supplied!
    uint64_t time_offset = update_time_frame();
    bool is_ok = (time_offset >=
                  rate_inv.load(std::memory_order_acquire) * curr_num_blks);
    if (is_ok) ++curr_num_blks;
    return is_ok;
  }

  // after allocator publish an allocation decision, there will be a tenant
  // needs some additional bandwidth to populate cache. We will temporally turn
  // off rate limiter after allocation to speed it up convergence. only called
  // by allocator.
  void turn(bool to_on) { is_on = to_on; }

  [[nodiscard]] bool is_min_bandwidth() const {
    return rate_inv.load(std::memory_order_acquire) >=
           params::cycles_per_second / params::min_bandwidth;
  }
};
}  // namespace sched
