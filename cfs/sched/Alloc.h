
#pragma once

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <thread>

#include "Log.h"
#include "Param.h"
#include "Resrc.h"
#include "Tenant.h"
#include "View.h"

class FsProc;

namespace sched {

class Allocator {
  FsProc* fs_proc;
  ResrcAlloc total_resrc;
  ResrcAlloc base_resrc;
  std::vector<AppResrcView> views;

 public:
  explicit Allocator(FsProc* fs_proc) : fs_proc(fs_proc) {}

  AppResrcView& append_view(int aid) {
    // currently require ordered by aid
    assert(size_t(aid) == views.size());
    views.emplace_back(aid);
    return views.back();
  }
  void add_total_resrc(ResrcAlloc r) {
    total_resrc += r;
    base_resrc = total_resrc / views.size();
  }

  [[noreturn]] static void run(Allocator* allocator);

 private:
  /**
   * @brief Do allocation. Note that our primary goal is to maximize the minimum
   * improvement, so we stop when this metric cannot be improved. It could be
   * the case that there is CPU left and some apps want more, but since we don't
   * do work-conserving, we may not allocate these available CPU to them.
   */
  void do_alloc();

  /**
   * @brief Harvest bandwidth by relocating cache.
   *
   * @return int64_t How much bandwidth is harvested.
   */
  int64_t do_harvest();

  /**
   * @brief Distribute the available CPU and bandwidth.
   *
   * @param cpu_avail CPU to distribute.
   * @param bw_avail Bandwidth to distribute; must be zero after return.
   * consumed.
   * @return int64_t How many CPU cycles is left undistributed.
   */
  int64_t do_distribute(int64_t cpu_avail, int64_t bw_avail);

  /**
   * @brief Apply the allocation result to the system.
   */
  void do_apply();

  void do_symm_partition();  // policy::symm_partition = true

  void do_asymm_partition_naive();  // policy::avoid_tiny_weight = false

  void do_asymm_partition_avoid_tiny();  // policy::avoid_tiny_weight = true

  void do_apply_to_app(AppResrcView& view);
};

struct AllocDecision {
  int aid;
  std::vector<std::tuple<int, int>> inode_move;  // dst_wid, num_files
  ResrcAlloc resrc;
};

[[noreturn]] inline void Allocator::run(Allocator* allocator) {
  SCHED_LOG_NOTICE("Allocator started");
  pthread_setname_np(pthread_self(), "Allocator");
  // wait for preheat (populating the cache may take some time)
  // if all apps start to make progress, we wait for `preheat_window_us`; if not
  // we wait until all apps to make progress
  bool are_all_active;
  for (auto& v : allocator->views) v.reset_stat();
  while (true) {
    are_all_active = true;
    for (auto& v : allocator->views)
      are_all_active &= v.poll_stat(/*silent*/ true);  // no log at this stage
    if (are_all_active) {
      // we wait for a relatively long time before start because the app needs
      // time to populate its cache.
      std::this_thread::sleep_for(
          std::chrono::microseconds(params::alloc::preheat_window_us));
      break;  // some apps start to make progress
    } else {
      std::this_thread::sleep_for(std::chrono::microseconds(1000));  // spin
    }
  }

  while (true) {
    for (auto& v : allocator->views) v.reset_stat();
    std::this_thread::sleep_for(
        std::chrono::microseconds(params::alloc::stat_coll_window_us));

    // we assume all apps must be active and trying to fully utilize the
    // resources; we don't have a very well support for a client that is not
    // active. if we detect a client does not make any progress, we assume the
    // system is not ready or in a unstable state, so we don't do allocation in
    // this case.
    are_all_active = true;
    for (auto& v : allocator->views) {
      bool is_active = v.poll_stat();
      if (!is_active) {
        SPDLOG_INFO("App {} is inactive", v.aid);
        SCHED_LOG_NOTICE("App %d is inactive", v.aid);
      }
      are_all_active &= is_active;
    }
    if (are_all_active) {
      // // dump ghost cache hit rates
      // for (auto& v : allocator->views) v.print();

      if (sched::params::policy::alloc_enabled) allocator->do_alloc();

      if constexpr (params::alloc::unlimited_bandwidth_window_us > 0) {
        // to speedup convergence, we allow tenants to use more bandwidth then
        // allocated to update their cache to a steady state
        SCHED_LOG_NOTICE("Turn off RateLimiter shortly after allocation");
        for (auto& v : allocator->views)
          v.turn_blk_rate_limiter(/*to_on*/ false);
        std::this_thread::sleep_for(std::chrono::microseconds(
            params::alloc::unlimited_bandwidth_window_us));
        SCHED_LOG_NOTICE("Turn RateLimiter back on");
        for (auto& v : allocator->views)
          v.turn_blk_rate_limiter(/*to_on*/ true);
        SCHED_LOG_NOTICE("All RateLimiter must be on");
      }
    } else {
      SCHED_LOG_NOTICE(
          "Some clients are inactive; no allocation will be done in this case");
      std::this_thread::sleep_for(std::chrono::microseconds(
          params::alloc::unlimited_bandwidth_window_us));  // sleep too...
    }

    std::this_thread::sleep_for(
        std::chrono::microseconds(params::alloc::stabilize_window_us));
  }
}

inline void Allocator::do_alloc() {
  if (views.size() <= 1) return;  // nothing to schedule if only one client

  // first, let all tenants' resources set to the equal case
  for (auto& v : views) v.set_resrc(base_resrc);

  SCHED_LOG_NOTICE("Baseline Resource: cache=%d, bw=%ld, cpu=%ld",
                   base_resrc.cache_size, base_resrc.bandwidth,
                   base_resrc.cpu_cycles);

  // available resources (either from collect_idle or harvest)
  int64_t cpu_avail = 0;  // unit: cycles
  int64_t bw_avail = 0;

  // collect idle resources
  for (auto& v : views) {
    auto [cpu_idle, bw_idle] = v.collect_idle();
    assert(cpu_idle >= 0);
    assert(bw_idle >= 0);
    cpu_avail += cpu_idle;
    bw_avail += bw_idle;
  }
  // now every resource must be fully utilized
  SCHED_LOG_NOTICE(
      "Allocator: Available resource after clearing idleness: cpu=%ld, bw=%ld",
      cpu_avail, bw_avail);

  // then start harvest
  if (params::policy::harvest_enabled && params::policy::cache_partition) {
    // if cache_partition is not enabled, we are using global LRU, so there is
    // no per-tenant cache allocation, thus, no harvest
    bw_avail += do_harvest();
    SCHED_LOG_NOTICE(
        "Allocator: Available resource after harvest: cpu=%ld, bw=%ld",
        cpu_avail, bw_avail);
  }

  assert(cpu_avail >= 0);
  assert(bw_avail >= 0);
  if (bw_avail == 0 && cpu_avail == 0) goto done;

  // distribute those harvested resources
  cpu_avail = do_distribute(cpu_avail, bw_avail);
  if (cpu_avail == 0) goto done;

  // if there are clients are full hit, they are not bottleneck on bandwidth, so
  // they could benefit from available CPUs.
  {
    assert(!params::policy::strict_weight_distr);
    int64_t full_hit_cpu_sum = 0;
    for (auto& v : views) {
      if (v.is_full_hit()) full_hit_cpu_sum += v.get_resrc().cpu_cycles;
    }
    if (full_hit_cpu_sum > 0) {
      int64_t cpu_avail_total = cpu_avail;
      for (auto& v : views) {
        if (v.is_full_hit()) {
          int64_t cpu_return = (double)cpu_avail_total / full_hit_cpu_sum *
                               v.get_resrc().cpu_cycles;
          v.add_cpu(cpu_return);
          cpu_avail -= cpu_return;
          assert(cpu_avail >= 0);
          SCHED_LOG_NOTICE(
              "Allocator: Give additional CPU to full-hit App %d: cpu=%ld",
              v.aid, cpu_return);
        }
      }
    }
  }

  // fallback: we return CPU to keep as close to baseline as possible
  for (auto& v : views) {
    assert(cpu_avail >= 0);
    if (cpu_avail == 0) break;
    int64_t cpu_diff =
        base_resrc.cpu_cycles - v.get_resrc().cpu_cycles;  // signed
    if (cpu_diff > 0) {
      int64_t cpu_return = std::min(cpu_diff, cpu_avail);
      cpu_avail -= cpu_return;
      SCHED_LOG_NOTICE("Allocator: Return back to App %d: cpu=%ld", v.aid,
                       cpu_return);
      v.add_cpu(cpu_return);
    }
  }

done:
  SCHED_LOG_NOTICE("=== Allocation Decision ===");
  for (auto& v : views) v.log_decision();
  do_apply();
}

inline int64_t Allocator::do_harvest() {
  int64_t bw_harvested = 0;

  // pair<bw_release/compensate, index>
  std::vector<std::pair<int64_t, int>> bw_rel_list;
  std::vector<std::pair<int64_t, int>> bw_comp_list;
  for (int i = 0; i < int(views.size()); ++i) {
    auto& v = views[i];
    bw_rel_list.emplace_back(v.pred_what_if_more_cache(), i);
    bw_comp_list.emplace_back(v.pred_what_if_less_cache(), i);
  }

  uint32_t trade_round = 0;
  [[maybe_unused]] auto t0 = std::chrono::high_resolution_clock::now();

  while (true) {
    if (trade_round >= params::max_trade_round) break;

    std::nth_element(bw_rel_list.begin(), bw_rel_list.begin(),
                     bw_rel_list.end(),
                     [](const std::pair<int64_t, int>& lhs,
                        const std::pair<int64_t, int>& rhs) {
                       return lhs.first > rhs.first;
                     });
    std::nth_element(bw_comp_list.begin(), bw_comp_list.begin(),
                     bw_comp_list.end(),
                     [](const std::pair<int64_t, int>& lhs,
                        const std::pair<int64_t, int>& rhs) {
                       return lhs.first < rhs.first;
                     });
    auto [bw_rel, rel_idx] = bw_rel_list[0];
    auto [bw_comp, comp_idx] = bw_comp_list[0];
    if (rel_idx == comp_idx) {
      // in a rare case, both release and compensate are from the same client,
      // in which case we just skip the compensate one (use the second instead)
      std::nth_element(bw_comp_list.begin(), bw_comp_list.begin() + 1,
                       bw_comp_list.end(),
                       [](const std::pair<int64_t, int>& lhs,
                          const std::pair<int64_t, int>& rhs) {
                         return lhs.first < rhs.first;
                       });
      bw_comp = bw_comp_list[1].first;
      comp_idx = bw_comp_list[1].second;
    }
    // likely no further deal can be made
    if (bw_rel - bw_comp <= params::min_bandwidth_harvest) break;

    auto& v_rel = views[rel_idx];
    auto& v_comp = views[comp_idx];

    SCHED_LOG_DEBUG("App-%d: bw -= %ld MB/s", v_rel.aid, params::blocks_to_mb_int(bw_rel));
    SCHED_LOG_DEBUG("App-%d: bw += %ld MB/s", v_comp.aid, params::blocks_to_mb_int(bw_comp));

    v_rel.add_cache_delta();
    v_comp.minus_cache_delta();
    v_rel.add_bandwidth(-bw_rel);
    v_comp.add_bandwidth(bw_comp);
    bw_harvested += bw_rel - bw_comp;

    // trigger the next round: recompute those prediction that has resources
    // updated
    int done_cnt = 0;  // there are four elements to update
    for (int i = 0; i < int(views.size()) && done_cnt < 4; ++i) {
      if (bw_rel_list[i].second == rel_idx ||
          bw_rel_list[i].second == comp_idx) {
        auto& v = views[bw_rel_list[i].second];
        bw_rel_list[i].first = v.pred_what_if_more_cache();
        ++done_cnt;
      }
      if (bw_comp_list[i].second == rel_idx ||
          bw_comp_list[i].second == comp_idx) {
        auto& v = views[bw_comp_list[i].second];
        bw_comp_list[i].first = v.pred_what_if_less_cache();
        ++done_cnt;
      }
    }
    ++trade_round;
  }
  [[maybe_unused]] auto t1 = std::chrono::high_resolution_clock::now();
  SCHED_LOG_NOTICE("Trading takes %.2lf us (%d rounds)",
                   std::chrono::duration<double, std::micro>(t1 - t0).count(),
                   trade_round);
  return bw_harvested;
}

inline int64_t Allocator::do_distribute(int64_t cpu_avail, int64_t bw_avail) {
  int64_t bw_sum = total_resrc.bandwidth - bw_avail;
  assert(bw_sum >= 0);
  double improve_ratio = 0;
  if (bw_sum > 0) {  // common case
    int64_t bw_avail_total = bw_avail;
    improve_ratio = double(bw_avail) / bw_sum;
    SCHED_LOG_NOTICE("Expect improvement after BE-distribution: %.2lf%%",
                     improve_ratio * 100);
    for (auto& v : views) {
      auto r = v.get_resrc();
      if (r.bandwidth == 0) continue;
      int64_t bw_distr = bw_avail_total * r.bandwidth / bw_sum;
      v.add_bandwidth(bw_distr);
      bw_avail -= bw_distr;
      assert(bw_avail >= 0);
    }
  } else {  // everyone is a hit... just share
    for (auto& v : views) v.add_bandwidth(bw_avail / views.size());
    bw_avail -= bw_avail / views.size() * views.size();
  }
  // this could happen due to rounding issue... just give it to a random clients
  if (bw_avail > 0) views[0].add_bandwidth(bw_avail);

  //  if (improve_ratio == 0) return cpu_avail;
  int64_t cpu_sum = total_resrc.cpu_cycles - cpu_avail;
  // I don't think it could be the case that all CPU are available...
  assert(cpu_sum > 0);
  if (params::policy::strict_weight_distr ||
      improve_ratio * cpu_sum > cpu_avail) {
    SCHED_LOG_NOTICE(
        "Expect improvement after CPU-distribution: %.2lf%%",
        std::min<double>(double(cpu_avail) / cpu_sum, improve_ratio) * 100);
    // use strict-weighted policy OR too much demand, have to share by weight
    int64_t cpu_avail_total = cpu_avail;
    for (auto& v : views) {
      auto r = v.get_resrc();
      int64_t cpu_distr = (double)cpu_avail_total / cpu_sum * r.cpu_cycles;
      v.add_cpu(cpu_distr);
      cpu_avail -= cpu_distr;
    }
    assert(cpu_avail >= 0);
    // due to rounding error
    if (cpu_avail > 0) views[0].add_cpu(cpu_avail);
    return 0;
  } else {  // only give CPU when necessary
    for (auto& v : views) {
      SCHED_LOG_NOTICE("Expect improvement after CPU-distribution: %.2lf%%",
                       improve_ratio * 100);
      auto r = v.get_resrc();
      int64_t cpu_distr = improve_ratio * r.cpu_cycles;
      v.add_cpu(cpu_distr);
      cpu_avail -= cpu_distr;
    }
    return cpu_avail;
  }
}
}  // namespace sched
