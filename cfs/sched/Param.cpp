#include "Param.h"

#include "spdlog/spdlog.h"

namespace sched::params {

namespace policy {

// red-button: whether do allocation or not; if false, the allocator will not
// perform any allocation after initialization (but still do stat collection)
bool alloc_enabled = true;

// whether enable resource harvest phase; if false, it is a cache-unawared DRF
bool harvest_enabled = true;

// whether perform symmetric resource partition among workers
bool symm_partition = true;

// when doing asymmetric resource partition, whether avoid over-small leftover
// weight on a worker, which could be less resistant to skewness
bool avoid_tiny_weight = true;

bool strict_cpu_usage = true;

bool cache_partition = true;

bool unlimited_bandwidth_if_unpopulated_cache = true;

}  // namespace policy

void log_params() {
  SPDLOG_INFO(
      "Policy flags: "
      "strict_weight_distr={}, "
      "alloc_enabled={}, "
      "harvest_enabled={}, "
      "symm_partition={}, "
      "avoid_tiny_weight={}, "
      "strict_cpu_usage={}, "
      "cache_partition={}, "
      "unlimited_bandwidth_if_unpopulated_cache={}",
      sched::params::policy::strict_weight_distr,
      sched::params::policy::alloc_enabled,
      sched::params::policy::harvest_enabled,
      sched::params::policy::symm_partition,
      sched::params::policy::avoid_tiny_weight,
      sched::params::policy::strict_cpu_usage,
      sched::params::policy::cache_partition, 
      sched::params::policy::unlimited_bandwidth_if_unpopulated_cache);
  SPDLOG_INFO(
      "Other params: "
      "cache_delta={}MB, "
      "min_cache_total={}MB, "
      "ghost::min_size={}MB, "
      "ghost::max_size={}MB, "
      "ghost::tick={}MB, "
      "alloc::preheat_window_us={}, "
      "alloc::freq_us={}, "
      "alloc::stat_coll_window_us={}, "
      "alloc::unlimited_bandwidth_window_us={}",
      blocks_to_mb_int(cache_delta), blocks_to_mb_int(min_cache_total),
      blocks_to_mb_int(ghost::min_size), blocks_to_mb_int(ghost::max_size),
      blocks_to_mb_int(ghost::tick), alloc::preheat_window_us, alloc::freq_us,
      alloc::stat_coll_window_us, alloc::unlimited_bandwidth_window_us);
}

}  // namespace sched::params
