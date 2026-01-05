#include "Alloc.h"

#include <cmath>
#include <cstddef>
#include <stdexcept>

#include "FsProc_Fs.h"
#include "FsProc_Messenger.h"
#include "Log.h"
#include "Param.h"
#include "Resrc.h"
#include "View.h"

namespace sched {

void Allocator::do_apply() {
  for (auto& view : views) view.reset_pending_weights();

  // update pending weights
  if (params::policy::symm_partition) {
    do_symm_partition();
  } else {
    if (params::policy::avoid_tiny_weight) {
      do_asymm_partition_avoid_tiny();
    } else {
      // the naive approch is simpler (potentially less error-prone)
      do_asymm_partition_naive();
    }
  }

  // apply pending weights
  for (auto& view : views) {
    view.log_pending_weights();
    do_apply_to_app(view);
  }
}

void Allocator::do_symm_partition() {
  int num_workers = fs_proc->getNumThreads();
  assert(int(views.size()) == fs_proc->getNumApps());

  uint32_t per_worker_avail_weight = params::worker_avail_weight;

  for (auto& view : views) {
    uint32_t per_worker_weight =
        view.get_pending_weight_unalloc() / num_workers;
    for (int wid = 0; wid < num_workers; ++wid)
      view.add_pending_weight(wid, per_worker_weight);
    per_worker_avail_weight -= per_worker_weight;
  }
  std::vector<uint32_t> workers_avail_weight(num_workers,
                                             params::worker_avail_weight);
  if (per_worker_avail_weight == 0) return;
  SCHED_LOG_NOTICE("leftover_weight=%d (fine if not too much)",
                   per_worker_avail_weight);
  // else: a rounding issue... just randomly distribute to preserve invariant...

  for (auto& view : views) {
    // assert: it must be a rounding issue...
    assert(view.get_pending_weight_unalloc() < num_workers);
    for (int wid = 0; wid < num_workers; ++wid) {
      if (view.get_pending_weight_unalloc() == 0) break;
      if (workers_avail_weight[wid] > 0) {
        view.add_pending_weight(wid, 1);
        workers_avail_weight[wid]--;
      }
    }
  }
}

void Allocator::do_asymm_partition_naive() {
  int num_workers = fs_proc->getNumThreads();
  assert(int(views.size()) == fs_proc->getNumApps());

  std::vector<uint32_t> workers_avail_weight;
  workers_avail_weight.reserve(num_workers);
  for (int wid = 0; wid < num_workers; ++wid)
    workers_avail_weight.emplace_back(params::worker_avail_weight);

  // pre-sort views by CPU-weight: we prefer to allocate CPU-bound apps first
  // pre-sorted order for allocation:
  // 1. we prefer tenants with <1 CPU, as they may be bandwidth-bounded, and it
  //    is a better idea to spread them to different worker
  // 2. otherwise, we prefer ones with more CPUs
  std::vector<AppResrcView*> views_sorted;
  for (auto& v : views) views_sorted.emplace_back(&v);
  std::sort(views_sorted.begin(), views_sorted.end(),
            [](const AppResrcView* lhs, const AppResrcView* rhs) {
              bool lhs_more_than_1cpu =
                  lhs->get_resrc().cpu_cycles >
                  static_cast<int64_t>(params::worker_avail_cycles_per_second);
              bool rhs_more_than_1cpu =
                  rhs->get_resrc().cpu_cycles >
                  static_cast<int64_t>(params::worker_avail_cycles_per_second);
              if (lhs_more_than_1cpu != rhs_more_than_1cpu)
                return rhs_more_than_1cpu;
              return lhs->get_resrc().cpu_cycles > rhs->get_resrc().cpu_cycles;
            });

  for (auto v_ptr : views_sorted) {
    auto& view = *v_ptr;
    std::vector<std::pair<int, uint32_t>> weights_distr_list;
    {
      std::vector<uint32_t> old_weights;
      view.get_weights(old_weights);  // filled the list
      for (int wid = 0; wid < num_workers; ++wid)
        weights_distr_list.emplace_back(wid, old_weights[wid]);
    }
    std::sort(weights_distr_list.begin(), weights_distr_list.end(),
              [](const std::pair<int, uint32_t>& lhs,
                 const std::pair<int, uint32_t>& rhs) {
                if (lhs.second != rhs.second) return lhs.second > rhs.second;
                return lhs.first < rhs.first;
              });

    // the greedy algorithm below could produce optimal results for two apps but
    // not in the case of more. we use this simple approach first.
    for (auto [wid, w_] : weights_distr_list) {
      if (view.get_pending_weight_unalloc() == 0) break;
      uint32_t& avail_weight = workers_avail_weight[wid];
      uint32_t alloc_weight =
          std::min(view.get_pending_weight_unalloc(), avail_weight);
      view.add_pending_weight(wid, alloc_weight);
      avail_weight -= alloc_weight;
    }
  }
}

void Allocator::do_asymm_partition_avoid_tiny() {
  int num_workers = fs_proc->getNumThreads();
  assert(int(views.size()) == fs_proc->getNumApps());

  // <wid, weight>
  std::vector<int> avail_dedi_workers;  // wid list
  for (int wid = 0; wid < num_workers; ++wid)
    avail_dedi_workers.emplace_back(wid);
  // invariant: a worker's available weight is either full (wid in
  // avail_dedi_workers) OR weight in workers_avail_weight
  std::vector<uint32_t> workers_avail_weight(num_workers, 0);

  // 1. allocate dedicated workers whenever possible
  for (auto& view : views) {
    std::vector<uint32_t> old_weights;
    view.get_weights(old_weights);
    // will pop from back, so put preferred one at the end
    std::sort(avail_dedi_workers.begin(), avail_dedi_workers.end(),
              [&](const int& lhs, const int& rhs) -> bool {
                return old_weights[lhs] != old_weights[rhs]
                           ? old_weights[lhs] < old_weights[rhs]
                           : lhs > rhs;
              });

    while (view.get_pending_weight_unalloc() >= params::worker_avail_weight) {
      assert(!avail_dedi_workers.empty());
      int wid = avail_dedi_workers.back();
      avail_dedi_workers.pop_back();
      view.add_pending_weight(wid, params::worker_avail_weight);
      SCHED_LOG_NOTICE("App-%d gets dedicated worker-%d", view.aid, wid);
    }
  }
  // now everyone should have enough dedicated workers
  // 2. figure out how to partition partial workers
  for (auto wid : avail_dedi_workers) {
    assert(workers_avail_weight[wid] == 0);
    workers_avail_weight[wid] = params::worker_avail_weight;
  }
  avail_dedi_workers.clear();

  // 2.1. check whether there will be "tiny" weight; handle it if so
  for (auto& view : views) {
    if (view.get_pending_weight_unalloc() < params::soft_min_weight) {
      SCHED_LOG_NOTICE(
          "App-%d has tiny weight leftover weight=%d; will try split-merge",
          view.aid, view.get_pending_weight_unalloc());
      // if there is a "tiny" weight, find one dedicated worker and only
      // return partial back; this makes our leftover allocation larger
      const auto& pending_weights = view.get_pending_weights();
      for (int wid = num_workers - 1; wid >= 0; --wid) {
        if (pending_weights[wid] == 0) continue;
        // find a dedicated worker and break it into half
        assert(pending_weights[wid] == params::worker_avail_weight);
        auto w_move =
            (pending_weights[wid] - view.get_pending_weight_unalloc()) / 2;
        view.add_pending_weight(wid, -w_move);
        workers_avail_weight[wid] += w_move;
        SCHED_LOG_NOTICE("App-%d splits dedicated worker-%d: return weight=%d",
                         view.aid, wid, w_move);
        break;
      }
    }
    // then find a way to fill the leftover allocation
    // we use this <wid, weight> list ordered by weights (secondary by wid)
    std::vector<std::pair<int, uint32_t>> curr_avail_list;  // <wid, weight>
    for (int wid = 0; wid < num_workers; ++wid)
      curr_avail_list.emplace_back(wid, workers_avail_weight[wid]);
    std::sort(curr_avail_list.begin(), curr_avail_list.end(),
              [&](const std::pair<int, uint32_t>& lhs,
                  const std::pair<int, uint32_t>& rhs) -> bool {
                if (lhs.second > rhs.second) return true;
                return lhs.first < rhs.first;
              });

    for (auto [wid, w_avail] : curr_avail_list) {
      if (view.get_pending_weight_unalloc() == 0) break;
      auto w_alloc = std::min(w_avail, view.get_pending_weight_unalloc());
      SCHED_LOG_NOTICE("App-%d places leftover weight=%d on worker-%d",
                       view.aid, w_alloc, wid);
      view.add_pending_weight(wid, w_alloc);
      workers_avail_weight[wid] -= w_alloc;
    }
  }
}

void Allocator::do_apply_to_app(AppResrcView& view) {
  assert(view.get_pending_weight_unalloc() == 0);
  int num_workers = fs_proc->getNumThreads();
  SCHED_LOG_NOTICE("=== Resource Distribution of App-%d ===", view.aid);

  std::vector<Tenant*> tenants = view.get_tenants();
  std::vector<int> nfiles_curr(num_workers);  // wid -> num_files
  for (auto& tenant : tenants) {
    auto app = tenant->get_app();
    auto wid = app->getWorker()->getWid();
    int n = app->GetInos().size();
    nfiles_curr[wid] = n;
  }

  auto& weights = view.get_pending_weights();

  const int total_num_files =
      std::accumulate(nfiles_curr.begin(), nfiles_curr.end(), 0);

  const uint32_t app_total_weight =
      std::accumulate(weights.begin(), weights.end(), 0);

  // distribute nfiles based on the weights and do proper rounding
  std::vector<int> nfiles_next(num_workers);  // wid -> num_files
  {
    // We first round down for nfiles_next
    std::vector<double> nfiles_weighted(num_workers);  // wid -> num_files
    std::vector<double> nfiles_diff(num_workers);      // wid -> num_files
    for (int wid = 0; wid < num_workers; ++wid) {
      nfiles_weighted[wid] =
          (double)total_num_files * weights[wid] / app_total_weight;
      nfiles_next[wid] = floor(nfiles_weighted[wid]);
      nfiles_diff[wid] = nfiles_weighted[wid] - nfiles_next[wid];
    }

    // Then distribute the remaining files to the workers with the largest
    // difference
    int nfiles_remain = total_num_files - std::accumulate(nfiles_next.begin(),
                                                          nfiles_next.end(), 0);
    for (int i = 0; i < nfiles_remain; i++) {
      auto max_it = std::max_element(nfiles_diff.begin(), nfiles_diff.end());
      int wid = std::distance(nfiles_diff.begin(), max_it);
      nfiles_next[wid]++;
      nfiles_diff[wid]--;
    }

    SCHED_LOG_NOTICE(
        "%s", fmt::format("App-{}: files curr=[{}], weighted=[{}], next=[{}]",
                          view.aid, fmt::join(nfiles_curr, ", "),
                          fmt::join(nfiles_weighted, ", "),
                          fmt::join(nfiles_next, ", "))
                  .c_str());
  }

  // Based on nfiles_curr and nfiles_next, compute the inode movement
  // src_wid -> vector of dst_wid, num_files
  std::vector<std::vector<std::tuple<int, int>>> inode_move(num_workers);
  {
    // First compute the source and destination workers
    std::vector<std::tuple<int, int>> src_apps;  // (wid, num_files)
    std::vector<std::tuple<int, int>> dst_apps;  // (wid, num_files)
    int num_files_left = total_num_files;
    for (int wid = 0; wid < num_workers; ++wid) {
      int curr_n = nfiles_curr[wid];
      int next_n = nfiles_next[wid];
      if (curr_n > next_n) {
        src_apps.emplace_back(wid, curr_n - next_n);
      } else if (curr_n < next_n) {
        dst_apps.emplace_back(wid, next_n - curr_n);
      }
      num_files_left -= next_n;
      if (num_files_left <= 0) {
        break;
      }
    }

    // Then compute the inode movement
    for (auto& [src_wid, n] : src_apps) {
      for (auto& [dst_wid, m] : dst_apps) {
        int num_files_to_migrate = std::min(n, m);
        m -= num_files_to_migrate;
        n -= num_files_to_migrate;
        inode_move[src_wid].emplace_back(dst_wid, num_files_to_migrate);
        if (n == 0) break;
      }
    }
  }

  for (int wid = 0; wid < num_workers; ++wid) {
    if (inode_move[wid].empty()) continue;
    if (params::policy::symm_partition) {
      SCHED_LOG_WARNING(
          "Unexpected inode movement: No inode migration is supposed to happen "
          "under symmetric partition policy (except when the benchmark is "
          "closing files). Migration ignored.");
      inode_move[wid].clear();
    }
    for ([[maybe_unused]] auto [dst_wid, nfiles] : inode_move[wid]) {
      SCHED_LOG_NOTICE("App-%d: move %d files from worker-%d to worker-%d",
                       view.aid, nfiles, wid, dst_wid);
    }
  }

  auto app_total_resrc = view.get_resrc();
  for (int wid = 0; wid < num_workers; ++wid) {
    FsProcMessage msg;
    msg.type = FsProcMessageType::kSCHED_NewResrcAlloc;
    auto decision = new AllocDecision{
        .aid = view.aid,
        .inode_move = std::move(inode_move[wid]),
        .resrc = {
            .cache_size = static_cast<uint32_t>(
                std::ceil(double(app_total_resrc.cache_size) *
                          nfiles_next[wid] / total_num_files)),
            .bandwidth = static_cast<int64_t>(
                std::ceil(double(app_total_resrc.bandwidth) * nfiles_next[wid] /
                          total_num_files)),
            .cpu_cycles =
                static_cast<int64_t>(params::weight_to_cycles(weights[wid])),
        }};

    SCHED_LOG_NOTICE("App-%d on Worker-%d: cache=%d, bw=%ld, cpu=%ld", view.aid,
                     wid, decision->resrc.cache_size, decision->resrc.bandwidth,
                     decision->resrc.cpu_cycles);
    msg.ctx = decision;
    fs_proc->messenger->send_message(wid, msg);
  }
  view.set_weights(weights);
}

}  // namespace sched
