#pragma once
#include <algorithm>
#include <cstdint>
#include <limits>

#include "Log.h"
#include "Param.h"
#include "Resrc.h"
#include "Tenant.h"
#include "gcache/ghost_cache.h"
#include "spdlog/spdlog.h"

namespace sched {

class GhostCacheView {
  const gcache::SampledGhostCache<>& ghost_cache;
  std::vector<HitRateCnt> prev_stat_image;
  std::vector<HitRateCnt> curr_stat_image;

 public:
  explicit GhostCacheView(const gcache::SampledGhostCache<>& ghost_cache)
      : ghost_cache(ghost_cache),
        prev_stat_image(params::ghost::num_ticks),
        curr_stat_image(params::ghost::num_ticks) {
    reset();
  }

  void reset();
  void poll();
  // the input cache may not be one of params::ghost::tick. if not, will
  // interpolate to get the cache hit rate
  HitRateCnt get_hit_rate_cnt(uint32_t cache_size);
};

// aggregate ghost cache view from different worker into one
class DistrGhostCacheView {
  uint32_t weight_sum;
  // the weight is supposed to mapped into
  std::vector<std::pair<uint32_t, GhostCacheView>> weighted_views;

  // if we have already computed the hit rate, keep it around
  std::unordered_map<uint32_t, double> hit_rate_map;

 public:
  DistrGhostCacheView() : weight_sum(0) {}
  // disallow copy
  DistrGhostCacheView(const DistrGhostCacheView&) = delete;
  DistrGhostCacheView& operator=(const DistrGhostCacheView&) = delete;
  // allow move
  DistrGhostCacheView(DistrGhostCacheView&&) = default;
  DistrGhostCacheView& operator=(DistrGhostCacheView&&) = default;

  // NOTE: such append ensures the ordering! the index will be used in
  // `update_weight`
  void append(const gcache::SampledGhostCache<>& ghost_cache, uint32_t weight);

  void update_weight(int idx, uint32_t weight);

  void reset();

  void poll();

  double get_hit_rate(uint32_t cache_size);

  HitRateCnt get_hit_rate_cnt(int wid, uint32_t cache_size);

  void print();
};

// an application's unified resource view
class AppResrcView {
  // track tenants on different workers (ordered by worker id)
  std::vector<Tenant*> tenants;
  // progress accounting
  std::vector<ResrcAcct> prev_prog;
  std::vector<ResrcAcct> curr_prog;
  // ghost cache tracking
  DistrGhostCacheView distr_ghost_cache_view;

  ResrcAlloc curr_resrc;

  // invariant: sum(pending_weights) + pending_weight_unalloc == total_weights
  uint32_t pending_weight_unalloc;
  std::vector<uint32_t> pending_weights;

  // updated in each `poll`: the current states of the workload
  int64_t cycles_per_block;
  double measured_miss_rate;  // from resource accounting, not from ghost cache

 public:
  const int aid;  // for logging and debugging

 public:
  AppResrcView(int aid) : aid(aid) {}
  // disallow clone
  AppResrcView(const AppResrcView&) = delete;
  AppResrcView& operator=(const AppResrcView&) = delete;
  // allow move
  AppResrcView(AppResrcView&&) = default;
  AppResrcView& operator=(AppResrcView&&) = default;
  // when calling `append_tenant`, `curr_resrc` shall be updated accordingly

  ResrcAlloc get_resrc() const { return curr_resrc; }
  void set_resrc(ResrcAlloc r) { curr_resrc = r; }

  // add a tenant; called durint the initialization
  void append_tenant(Tenant* t);

  const std::vector<Tenant*>& get_tenants() const { return tenants; }

  // return tuples of <wid, weight>
  void get_weights(std::vector<uint32_t>& weights) const;
  void set_weights(const std::vector<uint32_t>& weights);

  // take a snapshot of the current stat as the baseline
  void reset_stat();

  // poll the latest stat and diff it from the baseline
  bool poll_stat(bool silent = false);

  // either cpu or bandwidth may be underutilized. if that's true, these idle
  // resources will be collected before running `pred_what_if_*`
  // return <cpu, bw> pair; at least one of them should be zero.
  std::pair<int64_t, int64_t> collect_idle();

  // if given/taken cache, how much bandwidth to release/compensate to keep the
  // the same throughput (may be higher in the case of full cache hit...)
  int64_t pred_what_if_more_cache();
  int64_t pred_what_if_less_cache();

  // update resources
  void add_cache_delta() { curr_resrc.cache_size += params::cache_delta; }
  void minus_cache_delta() { curr_resrc.cache_size -= params::cache_delta; }
  void add_cpu(int64_t cycles) { curr_resrc.cpu_cycles += cycles; }
  void add_bandwidth(int64_t bandwidth) { curr_resrc.bandwidth += bandwidth; }

  void turn_blk_rate_limiter(bool to_on) {
    for (auto t : tenants) t->turn_blk_rate_limiter(to_on);
  }

  bool is_full_hit() {
    return distr_ghost_cache_view.get_hit_rate(curr_resrc.cache_size) >=
           params::full_hit_threshold;
  }

  void reset_pending_weights() {
    for (auto& w : pending_weights) w = 0;
    pending_weight_unalloc = params::cycles_to_weight(curr_resrc.cpu_cycles);
    SCHED_LOG_NOTICE("App-%d has pending weight=%d", aid,
                     pending_weight_unalloc);
  }
  void add_pending_weight(int wid, int weight_diff) {
    assert(pending_weights[wid] + weight_diff >= 0);
    pending_weights[wid] += weight_diff;
    pending_weight_unalloc -= weight_diff;
  }
  [[nodiscard]] const std::vector<uint32_t>& get_pending_weights() const {
    return pending_weights;
  }
  uint32_t get_pending_weight_unalloc() { return pending_weight_unalloc; }

  void log_decision();
  void log_pending_weights();

  void print();

 private:
  // predict the cpu demand to fully saturate bandwidth
  int64_t pred_cpu_demand() {
    double hit_rate =
        distr_ghost_cache_view.get_hit_rate(curr_resrc.cache_size);
    if (hit_rate >= params::full_hit_threshold)
      return std::numeric_limits<int64_t>::max();
    return curr_resrc.bandwidth * cycles_per_block / (1 - hit_rate);
  }
  // predict the bandwidth demand to fully saturate cpu
  int64_t pred_bandwidth_demand() {
    assert(cycles_per_block > 0);
    double hit_rate, miss_rate;
    if (params::policy::cache_partition) {
      hit_rate = distr_ghost_cache_view.get_hit_rate(curr_resrc.cache_size);
      if (hit_rate == std::numeric_limits<double>::infinity()) return 0;
      miss_rate = 1 - hit_rate;
      if (measured_miss_rate != std::numeric_limits<double>::infinity()) {
        // we use miss rate here because it is the actual metric being used
        double error = (miss_rate - measured_miss_rate) / measured_miss_rate;
        if (error > 0.05 || error < -0.05) {
          SCHED_LOG_WARNING(
              "Mismatch between measured miss rate and ghost-estimated miss "
              "rate: measured=%5.2lf%%, estimated=%5.2lf%%",
              measured_miss_rate * 100, miss_rate * 100);
        }
      }
    } else {
      if (measured_miss_rate == std::numeric_limits<double>::infinity())
        return 0;
      hit_rate = 1 - measured_miss_rate;
      miss_rate = measured_miss_rate;
    }
    if (hit_rate >= params::full_hit_threshold) return 0;
    return curr_resrc.cpu_cycles * miss_rate / cycles_per_block;
  }
};

inline void GhostCacheView::reset() {
  uint32_t size, i;
  for (size = params::ghost::min_size, i = 0; size <= params::ghost::max_size;
       size += params::ghost::tick, ++i)
    prev_stat_image[i] = ghost_cache.get_stat(size);
}

inline void GhostCacheView::poll() {
  uint32_t size, i;
  for (size = params::ghost::min_size, i = 0; size <= params::ghost::max_size;
       size += params::ghost::tick, ++i) {
    HitRateCnt s = ghost_cache.get_stat(size);
    curr_stat_image[i] = s - prev_stat_image[i];
    // we additionally ensure the hit rate must be inclusive
    // since we are polling from GhostCache, which could be updated by the
    // worker thread, we may not see the consistent data; however, we don't
    // really care for such strong consistency, but we should make sure some
    // basic property of the view hold (e.g., inclusive: a larger cache must
    // not have fewer hit count).
    if (i > 0) {
      if (curr_stat_image[i].hit_cnt < curr_stat_image[i - 1].hit_cnt)
        curr_stat_image[i].hit_cnt = curr_stat_image[i - 1].hit_cnt;
      if (curr_stat_image[i].miss_cnt > curr_stat_image[i - 1].miss_cnt)
        curr_stat_image[i].miss_cnt = curr_stat_image[i - 1].miss_cnt;
    } else {
      auto num_ops = s.miss_cnt + s.hit_cnt;
      //      if (num_ops > 0) SCHED_LOG_DEBUG("Ghost cache #ops: %ld",
      //      num_ops);
    }
  }
}

inline HitRateCnt GhostCacheView::get_hit_rate_cnt(uint32_t cache_size) {
  assert(cache_size <= params::ghost::max_size);
  if (cache_size < params::ghost::min_size) {
    double size_ratio = double(cache_size) / params::ghost::min_size;
    return HitRateCnt(curr_stat_image[0].hit_cnt * size_ratio,
                      curr_stat_image[0].hit_cnt * (1 - size_ratio) +
                          curr_stat_image[0].miss_cnt);
  }
  auto idx = (cache_size - params::ghost::min_size) / params::ghost::tick;
  uint32_t left_size = idx * params::ghost::tick + params::ghost::min_size;
  if (cache_size == left_size) return curr_stat_image[idx];
  auto& l_stat = curr_stat_image[idx];
  auto& r_stat = curr_stat_image[idx + 1];

  double l_dist = cache_size - left_size;
  double r_dist = left_size + params::ghost::tick - cache_size;
  assert(left_size < cache_size &&
         cache_size < (left_size + params::ghost::tick));
  double l_ratio = r_dist / (l_dist + r_dist);
  double r_ratio = l_dist / (l_dist + r_dist);
  return HitRateCnt(l_stat.hit_cnt * l_ratio + r_stat.hit_cnt * r_ratio,
                    l_stat.miss_cnt * l_ratio + r_stat.miss_cnt * r_ratio);
}

inline void DistrGhostCacheView::append(
    const gcache::SampledGhostCache<>& ghost_cache, uint32_t weight) {
  assert(weight <= params::max_weight);
  weighted_views.emplace_back(weight, ghost_cache);
  weight_sum += weight;
}

inline void DistrGhostCacheView::reset() {
  for (auto& [w, gcv] : weighted_views) gcv.reset();
}

inline void DistrGhostCacheView::poll() {
  for (auto& [w, gcv] : weighted_views) gcv.poll();
  hit_rate_map.clear();
}

inline void DistrGhostCacheView::update_weight(int idx, uint32_t weight) {
  assert(weight <= params::max_weight);
  auto& p = weighted_views[idx];
  weight_sum = weight_sum - p.first + weight;
  p.first = weight;
}

inline double DistrGhostCacheView::get_hit_rate(uint32_t cache_size) {
  auto it = hit_rate_map.find(cache_size);
  if (it != hit_rate_map.end()) return it->second;
  assert(weight_sum > 0);
  HitRateCnt hrc;
  for (size_t i = 0; i < weighted_views.size(); ++i) {
    auto& [w, gcv] = weighted_views[i];
    if (w) {
      hrc += gcv.get_hit_rate_cnt(w * cache_size / weight_sum);
      SPDLOG_DEBUG("W-{}: {} MB cache: hit = {}, miss = {}, hit_rate = {}", i,
                   params::blocks_to_mb(w * cache_size / weight_sum),
                   hrc.hit_cnt, hrc.miss_cnt, hrc.get_hit_rate());
    }
  }
  auto hit_rate = hrc.get_hit_rate();
  hit_rate_map.emplace(cache_size, hit_rate);
  return hit_rate;
}

inline HitRateCnt DistrGhostCacheView::get_hit_rate_cnt(int wid,
                                                        uint32_t cache_size) {
  return weighted_views[wid].second.get_hit_rate_cnt(cache_size);
}

inline void DistrGhostCacheView::print() {
  for (size_t i = 0; i < weighted_views.size(); ++i) {
    auto& [w, gcv] = weighted_views[i];
    if (!w) continue;
    for (uint32_t cache_size = params::ghost::min_size;
         cache_size <= params::ghost::max_size;
         cache_size += params::ghost::tick) {
      [[maybe_unused]] auto hrc = gcv.get_hit_rate_cnt(cache_size);
      SCHED_LOG_NOTICE(
          "W-%ld, %4ld MB: %5ld hit, %5ld miss -> %.3lf hit rate (w=%.2lf)", i,
          params::blocks_to_mb_int(cache_size), hrc.hit_cnt, hrc.miss_cnt,
          hrc.get_hit_rate(), double(w) / weight_sum);
    }
  }
}

inline void AppResrcView::append_tenant(Tenant* t) {
  tenants.emplace_back(t);
  prev_prog.emplace_back();
  curr_prog.emplace_back();
  distr_ghost_cache_view.append(t->resrc_ctrl_block.ghost_cache,
                                t->get_allocated_weight());
  curr_resrc += t->resrc_ctrl_block.curr_resrc;
  pending_weights.emplace_back(0);
}

inline void AppResrcView::get_weights(std::vector<uint32_t>& weights) const {
  weights.reserve(tenants.size());
  for (int wid = 0; wid < (int)tenants.size(); ++wid)
    weights.emplace_back(tenants[wid]->get_allocated_weight());
}

inline void AppResrcView::set_weights(const std::vector<uint32_t>& weights) {
  for (int wid = 0; wid < int(weights.size()); ++wid)
    distr_ghost_cache_view.update_weight(wid, weights[wid]);
}

inline void AppResrcView::reset_stat() {
  for (int i = 0; i < int(tenants.size()); ++i)
    prev_prog[i] = tenants[i]->resrc_acct;
  distr_ghost_cache_view.reset();
}

inline std::pair<int64_t, int64_t> AppResrcView::collect_idle() {
  int64_t bw_demand = pred_bandwidth_demand();
  int64_t bw_idle = curr_resrc.bandwidth - bw_demand;
  if (bw_idle > params::min_bandwidth) {
    curr_resrc.bandwidth = bw_demand;
    SCHED_LOG_NOTICE(
        "App-%d: Idleness: cpu=0, bw=%ld; "
        "current resource: {cache=%d, bw=%ld, cpu=%ld}",
        aid, bw_idle, curr_resrc.cache_size, curr_resrc.bandwidth,
        curr_resrc.cpu_cycles);
    return {0, bw_idle};
  }
  int64_t cpu_demand = pred_cpu_demand();
  int64_t cpu_idle = curr_resrc.cpu_cycles - cpu_demand;
  if (cpu_idle > 0) {
    curr_resrc.cpu_cycles = cpu_demand;
    SCHED_LOG_NOTICE(
        "App-%d: Idleness: cpu=%ld, bw=0; "
        "current resource: {cache=%d, bw=%ld, cpu=%ld}",
        aid, cpu_idle, curr_resrc.cache_size, curr_resrc.bandwidth,
        curr_resrc.cpu_cycles);
    return {cpu_idle, 0};
  }
  SCHED_LOG_NOTICE(
      "App-%d: Idleness: cpu=0, bw=0, ; "
      "current resource: {cache=%d, bw=%ld, cpu=%ld}",
      aid, curr_resrc.cache_size, curr_resrc.bandwidth, curr_resrc.cpu_cycles);
  return {0, 0};  // in case of rounding error
}

inline int64_t AppResrcView::pred_what_if_more_cache() {
  // returning 0 indicates to abort this deal.
  // this means this client is asking for cache but return with no bandwidth,
  // which is impossible to be accepted.
  constexpr static int64_t abort_offer = 0;
  double old_hit_rate =
      distr_ghost_cache_view.get_hit_rate(curr_resrc.cache_size);
  if (old_hit_rate >= params::full_hit_threshold ||
      old_hit_rate == std::numeric_limits<double>::infinity())
    return abort_offer;

  double new_hit_rate = distr_ghost_cache_view.get_hit_rate(
      curr_resrc.cache_size + params::cache_delta);
  if (new_hit_rate == std::numeric_limits<double>::infinity())
    return abort_offer;

  // cache hit rate can only be increasing, not decreasing
  assert(old_hit_rate <= new_hit_rate);

  int64_t bandwidth_release =
      curr_resrc.bandwidth * (new_hit_rate - old_hit_rate) / (1 - old_hit_rate);
  SCHED_LOG_NOTICE(
      "App-%d: cache %4ld + %ld MB"
      " ==> hit %.3lf -> %.3lf"
      " ==> bw %4ld - %3ld MB/s",
      aid, params::blocks_to_mb_int(curr_resrc.cache_size),
      params::blocks_to_mb_int(params::cache_delta),
      old_hit_rate, new_hit_rate,
      params::blocks_to_mb_int(curr_resrc.bandwidth),
      params::blocks_to_mb_int(bandwidth_release));
  assert(bandwidth_release >= 0);
  return bandwidth_release;
}

inline int64_t AppResrcView::pred_what_if_less_cache() {
  // returning int64_max indicates to abort this deal.
  // in other words, this client asks for the bandwidth compensation that no one
  // could possibly afford.
  constexpr static int64_t abort_offer = std::numeric_limits<int64_t>::max();
  if (curr_resrc.cache_size <= params::min_cache_total) return abort_offer;

  double old_hit_rate =
      distr_ghost_cache_view.get_hit_rate(curr_resrc.cache_size);
  if (old_hit_rate == std::numeric_limits<double>::infinity())
    return abort_offer;

  double new_hit_rate = distr_ghost_cache_view.get_hit_rate(
      curr_resrc.cache_size - params::cache_delta);
  if (new_hit_rate == std::numeric_limits<double>::infinity())
    return abort_offer;

  // cache hit rate can only be decreasing, not increasing
  assert(old_hit_rate >= new_hit_rate);

  int64_t bandwidth_compensate;

  // do not reorder these if-conditions! order matters
  if (new_hit_rate >= params::full_hit_threshold)
    bandwidth_compensate = 0;  // still full hit
  else if (old_hit_rate >= params::full_hit_threshold)
    return abort_offer;
  else
    bandwidth_compensate = curr_resrc.bandwidth *
                           (old_hit_rate - new_hit_rate) / (1 - old_hit_rate);
  SCHED_LOG_NOTICE(
        "App-%d: cache %4ld - %ld MB"
        " ==> hit %.3lf -> %.3lf"
        " ==> bw %4ld + %3ld MB/s",
        aid, params::blocks_to_mb_int(curr_resrc.cache_size),
        params::blocks_to_mb_int(params::cache_delta),
        old_hit_rate, new_hit_rate,
        params::blocks_to_mb_int(curr_resrc.bandwidth),
        params::blocks_to_mb_int(bandwidth_compensate));
  assert(bandwidth_compensate >= 0);
  return bandwidth_compensate;
}

inline void AppResrcView::log_decision() {
  auto hit_rate = distr_ghost_cache_view.get_hit_rate(curr_resrc.cache_size);
  SCHED_LOG_NOTICE(
      "Alloc Decision: App-%d: cache=%d, bw=%ld, cpu=%ld, hit_rate=%lf; "
      "cache_mb=%ldMB, bw_mbps=%ldMB/s, cpu_cnt=%lf",
      aid, curr_resrc.cache_size, curr_resrc.bandwidth, curr_resrc.cpu_cycles,
      hit_rate, params::blocks_to_mb_int(curr_resrc.cache_size),
      params::blocks_to_mb_int(curr_resrc.bandwidth),
      double(curr_resrc.cpu_cycles) / params::worker_avail_cycles_per_second);
}

inline void AppResrcView::log_pending_weights() {
  for (int wid = 0; wid < int(pending_weights.size()); ++wid)
    SCHED_LOG_NOTICE(
        "App-%d weight on Worker-%d: %d (cpu_cnt=%lf)", aid, wid,
        pending_weights[wid],
        double(pending_weights[wid]) / params::worker_avail_weight);
  if (pending_weight_unalloc)
    SCHED_LOG_WARNING("App-%d has unallocated weight: %d", aid,
                      pending_weight_unalloc);
}

inline void AppResrcView::print() {
  SCHED_LOG_NOTICE("=== Ghost Cache Dump for App-%d ===", aid);
  distr_ghost_cache_view.print();
}
}  // namespace sched
