#pragma once

#include <cassert>
#include <cstdint>
#include <iostream>
#include <limits>

#include "RateLimit.h"
#include "gcache/ghost_cache.h"

namespace sched {

// NOTE: for simplicity, we use cycles as the unit for time and #blocks for IO
//       the user input may use `MB' and `MB/s' as the unit but should be
//       translated into cycles and #block at ctor
struct ResrcAlloc {
  uint32_t cache_size = 0;  // unit: #blocks
  int64_t bandwidth = 0;    // unit: #blocks/second
  int64_t cpu_cycles = 0;   // unit: #cycles/second

  ResrcAlloc operator+(const ResrcAlloc& other) {
    return {cache_size + other.cache_size, bandwidth + other.bandwidth,
            cpu_cycles + other.cpu_cycles};
  }

  ResrcAlloc& operator+=(const ResrcAlloc& other) {
    cache_size += other.cache_size;
    bandwidth += other.bandwidth;
    cpu_cycles += other.cpu_cycles;
    return *this;
  }

  ResrcAlloc operator/(int div) {  // useful for equally share resource
    return {cache_size / div, bandwidth / div, cpu_cycles / div};
  }
};

// accounting: for now we assume each tenant's flow is stable, so we only
// record the total value; in the future when allocation happens dynamically,
// more mature accounting is necessary
struct ResrcAcct {
  int64_t num_blks_done;  // for throughput
  int64_t bw_consump;     // blocks
  int64_t cpu_consump;    // cycles

  ResrcAcct() : num_blks_done(0), bw_consump(0), cpu_consump(0) {}
  ResrcAcct(int64_t num_blks_done, int64_t bw_consump, int64_t cpu_consump)
      : num_blks_done(num_blks_done),
        bw_consump(bw_consump),
        cpu_consump(cpu_consump) {}

  ResrcAcct operator-(const ResrcAcct& other) {
    assert(num_blks_done >= other.num_blks_done);
    assert(bw_consump >= other.bw_consump);
    assert(cpu_consump >= other.cpu_consump);
    return ResrcAcct(num_blks_done - other.num_blks_done,
                     bw_consump - other.bw_consump,
                     cpu_consump - other.cpu_consump);
  }

  ResrcAcct operator+=(const ResrcAcct& other) {
    num_blks_done += other.num_blks_done;
    bw_consump += other.bw_consump;
    cpu_consump += other.cpu_consump;
    return *this;
  }

  friend std::ostream& operator<<(std::ostream& os, const ResrcAcct& r) {
    return os << "[done=" << r.num_blks_done << ",bw=" << r.bw_consump
              << ",cpu=" << r.cpu_consump << "]";
  }
};

// compatible with gcache::CacheStat but with more handy operator overloading
struct HitRateCnt {
  uint64_t hit_cnt;
  uint64_t miss_cnt;

  HitRateCnt() : hit_cnt(0), miss_cnt(0) {}
  HitRateCnt(uint64_t hit_cnt, uint64_t miss_cnt)
      : hit_cnt(hit_cnt), miss_cnt(miss_cnt) {}
  HitRateCnt(const gcache::CacheStat& cs)
      : hit_cnt(cs.hit_cnt), miss_cnt(cs.miss_cnt) {}

  double get_hit_rate() {
    uint64_t acc_cnt = hit_cnt + miss_cnt;
    if (acc_cnt == 0) return std::numeric_limits<double>::infinity();
    return double(hit_cnt) / double(acc_cnt);
  }

  HitRateCnt& operator=(const HitRateCnt& other) = default;
  HitRateCnt& operator=(const gcache::CacheStat& other) {
    hit_cnt = other.hit_cnt;
    miss_cnt = other.miss_cnt;
    return *this;
  }
  HitRateCnt operator+(const HitRateCnt& other) {
    return HitRateCnt(hit_cnt + other.hit_cnt, miss_cnt + other.miss_cnt);
  }
  HitRateCnt operator-(const HitRateCnt& other) {
    assert(hit_cnt >= other.hit_cnt);
    assert(miss_cnt >= other.miss_cnt);
    return HitRateCnt(hit_cnt - other.hit_cnt, miss_cnt - other.miss_cnt);
  }
  HitRateCnt& operator+=(const HitRateCnt& other) {
    hit_cnt += other.hit_cnt;
    miss_cnt += other.miss_cnt;
    return *this;
  }

  friend std::ostream& operator<<(std::ostream& os, const HitRateCnt& hrc) {
    return os << "[hit=" << hrc.hit_cnt << ",miss=" << hrc.miss_cnt << "]";
  }
};

struct ResrcCtrlBlock {
  // allocated resource
  ResrcAlloc curr_resrc;
  // limit submission rate for block request
  RateLimiter blk_rate_limiter;
  // use default sample rate
  gcache::SampledGhostCache<> ghost_cache;

  ResrcCtrlBlock(uint32_t cache_size, int64_t bandwidth, int64_t cpu_cycles)
      : curr_resrc({cache_size, bandwidth, cpu_cycles}),
        blk_rate_limiter(bandwidth),
        ghost_cache(params::ghost::tick, params::ghost::min_size,
                    params::ghost::max_size) {}

  void report_ghost_cache(std::ostream& report_buf) const {
    for (uint32_t c = ghost_cache.get_min_size();
         c <= ghost_cache.get_max_size(); c += ghost_cache.get_tick()) {
      auto& s = ghost_cache.get_stat(c);
      report_buf << "" << params::blocks_to_mb_int(c) << ": " << s << '\n';
    }
  }
};

}  // namespace sched
