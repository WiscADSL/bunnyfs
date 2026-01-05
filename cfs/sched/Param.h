#pragma once

#include <cassert>
#include <cstdint>
#include <limits>

namespace sched::params {

// policy flags (everything under `policy::` must be a bool flag)
namespace policy {

// whether perform strict weight distribution; if false, will try to give
// unallocated CPU to full-hit tenants, which does not improvement the fairness
// metric, but will improve some tenants
constexpr static bool strict_weight_distr = true;

// red-button: whether do allocation or not; if false, the allocator will sleep
// forever after initialization
extern bool alloc_enabled;

// whether enable resource harvest phase; if false, it is a cache-unawared DRF
// this flag is set by cmdline
extern bool harvest_enabled;

// whether perform symmetric resource partition among workers; if true, will
// spread resources and files evenly to every worker; if false, will try to
// allocate dedicated workers
extern bool symm_partition;

// when doing asymmetric resource partition, whether avoid over-small leftover
// weight on a worker, which could be less resistant to skewness
extern bool avoid_tiny_weight;

// whether to strictly enforce CPU usage limit or simply doing work-conserving
// with weight; if true, for a tenant allocated x% of CPU, will no longer
// process this tenant's request if it has used >= x% within an epoch
extern bool strict_cpu_usage;

// whether partition cache to each tenant or using a global cache
extern bool cache_partition;

// whether allows a tenant to have unthrottled bandwidth when its cache is not
// fully populated
// if having unpopulated cache, it is likely that this tenant just gets extra
// cache space and need to populate the cache to stabilize; we are sure that the
// upper bound of bandwidth this tenant could consume without rate limit is the
// unpopulated cache space
// NOTE: this flag does not work well for write-heavy workload, which may
// consume massive write bandwidth while maintaining unpopulated cache
// TODO: use separated rate limiting mechanisms for read and write bandwidth
extern bool unlimited_bandwidth_if_unpopulated_cache;

}  // namespace policy

/** Some independent parameters **/

// in each workerRunLoopInner, process how many requests; this controls the
// ratio between request processing and other work (e.g., submit to/poll from
// the device, etc)
constexpr static int num_reqs_per_loop = 3;

// if a hit rate is larger than this, we consider this client as all hit; this
// helps to solve the problem of rounding error of float-point number
constexpr static double full_hit_threshold = 0.999;

// stop if the trading has reached the max round limit
constexpr static uint32_t max_trade_round =
    std::numeric_limits<uint32_t>::max();

/** CPU/weight-related parameters **/

// NOTE: rdtsc has stable frequency, which differs from the actual CPU frequency
// check `lscpu | grep 'Model name'` to see (e.g., xxx CPU @ 2.10GHz)
constexpr static uint64_t cycles_per_second = 2'100UL * 1'000'000UL;

// we reset each app's progress after every 0.1 second
constexpr static uint64_t cycles_per_cpu_epoch = cycles_per_second / 10;

// resources are distributed to different worker in proportion to its cpu share;
// we translate such cpu_share into weight (note we assume no CPU's frequency is
// beyond 8 GHz)
constexpr static uint32_t max_weight = 8192;
// we set a min weight so that even we don't expect this server to process any
// request from this client, we still check this client sometimes in case any
// control plan operations necessary
constexpr static uint32_t min_weight = 1;

constexpr static inline uint32_t cycles_to_weight(uint64_t c) {
  assert((c >> 20) <= max_weight);
  return c >> 20;
}
constexpr static inline uint64_t weight_to_cycles(uint32_t w) {
  assert(w <= max_weight);
  return uint64_t(w) << 20;
}
constexpr static inline uint64_t cycles_to_progress(uint64_t c, uint32_t w) {
  return c * max_weight / w;
}
// reverse of cycles_to_progress
constexpr static inline uint64_t progress_to_cycles(uint64_t p, uint32_t w) {
  return p * w / max_weight;
}

constexpr static inline double cycles_to_seconds(uint64_t c) {
  return double(c) / cycles_per_second;
}
constexpr static inline uint64_t seconds_to_cycles(double s) {
  return s * cycles_per_second;
}

// note that many cycles are not accounted as each request's cost by the
// workers, e.g., enqueue/dequeue; we exclude these costs to know the real
// available cycles
constexpr static uint32_t worker_avail_weight =
    cycles_to_weight(1'900UL * 1'000'000UL);
constexpr static uint64_t worker_avail_cycles_per_second =
    weight_to_cycles(worker_avail_weight);
// this is a soft-constraint: lower than this weight may be too vulnerable to
// hotness skewness
constexpr static uint32_t soft_min_weight = worker_avail_weight * 0.2;

/** cache/bandwidth-related parameters **/

constexpr static inline double blocks_to_mb(uint64_t blocks) {
  return double(blocks) / 256;
}
constexpr static inline uint64_t blocks_to_mb_int(uint64_t blocks) {
  return blocks / 256;  // no float-point number
}
constexpr static inline uint64_t mb_to_blocks(double mb) { return mb * 256; }

#ifdef ALLOC_FINE_GRAINED
constexpr static uint32_t cache_delta = mb_to_blocks(4);
#else
constexpr static uint32_t cache_delta = mb_to_blocks(32);
#endif

// limite the least amount of cache that a tenant could have (no more trading
// beyond this point)
constexpr static uint32_t min_cache_total = cache_delta;

// the minimum bandwidth; this ensures a client could still make progress even
// the allocator "thinks" it is fully hit and does not need any bandwidth; this
// is only for one worker
constexpr static int64_t min_bandwidth = 256;  // ~1 MB/s
// similar reasons for cache
constexpr static uint32_t min_cache = 128;  // 0.5 MB

// if a deal harvest < 0.8 MB/s of bandwidth, stop the deal; this could make the
// algorithm more stable (converge to similar spot for stable workload)
constexpr static uint32_t min_bandwidth_harvest = 200;

/* Allocator parameters */
namespace alloc {
// the first allocation happens at 15s
#ifdef ALLOC_HIGH_FREQ
constexpr static uint64_t preheat_window_us = 14'500'000UL;  // 14.5 s
constexpr static uint64_t freq_us = 1'000'000UL;             // 1 s
constexpr static uint64_t stat_coll_window_us = 800'000UL;   // 0.8 s
#else
constexpr static uint64_t preheat_window_us = 10'000'000UL;   // 10 s
constexpr static uint64_t freq_us = 30'000'000UL;             // 30 s
constexpr static uint64_t stat_coll_window_us = 5'000'000UL;  // 5 s
#endif
constexpr static uint64_t unlimited_bandwidth_window_us = 0UL;  // disableds
constexpr static uint64_t stabilize_window_us =
    freq_us - stat_coll_window_us - unlimited_bandwidth_window_us;
static_assert(freq_us >= stat_coll_window_us + unlimited_bandwidth_window_us,
              "Allocation is too frequent!");
}  // namespace alloc

/* GhostCache parameters */
namespace ghost {

#ifdef ALLOC_FINE_GRAINED
constexpr static uint32_t min_size = mb_to_blocks(8);
constexpr static uint32_t max_size = mb_to_blocks(256);
constexpr static uint32_t tick = mb_to_blocks(8);
#else
constexpr static uint32_t min_size = mb_to_blocks(32);
constexpr static uint32_t max_size = mb_to_blocks(1024);
constexpr static uint32_t tick = mb_to_blocks(32);
#endif

constexpr static uint32_t num_ticks = (max_size - min_size) / tick + 1;
static_assert((max_size - min_size) % tick == 0,
              "Ghost cache max/min difference must be multiple of tick!");
}  // namespace ghost

/* RateLimiter parameters */
namespace rate {
constexpr static uint64_t cycles_per_frame = 1024UL * 1024UL * 256UL;  // ~0.12s
}  // namespace rate

// log down all compile-time/runtime mutable and other major params
void log_params();

}  // namespace sched::params
