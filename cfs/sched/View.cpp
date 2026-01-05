#include "View.h"

#include <limits>

#include "FsProc_App.h"
#include "Log.h"
#include "Param.h"

namespace sched {

// clang-format off
#define HEADER0 "========================================= App %d in the last %.0lf seconds ========================================="
#define HEADER1 "    |  TP   |     Cache    |       Ghost Cache        |         BW GB/s        |           CPU         | Inode "
#define HEADER2 "    |  GB/s |    GB, miss%% | hit_cnt, miss_cnt, miss%% | alloc,  used, tp*miss%% | alloc,  used, cyc/blk | Count "
#define ROW_FMT "%s | %5.3lf | %5.3lf, %5.1lf | %7ld, %8ld, %5.1lf | %5.3lf, %5.3lf, %8.3lf | %5.2lf, %5.2lf, %6.0lf  |  %3ld  "
// clang-format on

constexpr auto window = params::alloc::stat_coll_window_us / 1e6;
constexpr auto blk_to_gb = [](uint64_t blocks) {
  return double(blocks) / (256 * 1024);
};
constexpr auto blk_to_gbps = [](uint64_t blocks) {
  return double(blocks) / (256 * 1024 * window);
};
constexpr auto cyc_to_cnt = [](uint64_t cycles) {
  return double(cycles) / params::worker_avail_cycles_per_second;
};

struct Stat {
  const ResrcAcct& p;
  const ResrcAlloc& a;
  const HitRateCnt& c;
  size_t num_inodes = 0;

  void print(const std::string& name) const {
    auto tp_gbps = blk_to_gbps(p.num_blks_done);
    auto bw_gbps = blk_to_gbps(p.bw_consump);
    auto cpu_cnt = cyc_to_cnt(p.cpu_consump / window);
    auto cyc_per_blk =
        p.num_blks_done == 0 ? 0.0 : p.cpu_consump / double(p.num_blks_done);
    auto alloc_cache_gb = blk_to_gb(a.cache_size);
    auto alloc_bw_gb = blk_to_gb(a.bandwidth);
    auto alloc_cpu_cnt = cyc_to_cnt(a.cpu_cycles);
    auto hit = c.hit_cnt;
    auto miss = c.miss_cnt;
    auto miss_rate = (hit == 0) ? 1. : 1. * miss / (hit + miss);
    auto measured_miss_rate =
        (p.num_blks_done == 0) ? 1. : double(p.bw_consump) / p.num_blks_done;
    auto bw_tp_x_miss_rate = tp_gbps * miss_rate;

    SCHED_LOG_NOTICE(ROW_FMT, name.c_str(), tp_gbps, alloc_cache_gb,
                     measured_miss_rate * 100, hit, miss, miss_rate * 100,
                     alloc_bw_gb, bw_gbps, bw_tp_x_miss_rate, alloc_cpu_cnt,
                     cpu_cnt, cyc_per_blk, num_inodes);
  }
};

bool AppResrcView::poll_stat(bool silent) {
  ResrcAcct total{};
  distr_ghost_cache_view.poll();
  for (int i = 0; i < int(tenants.size()); ++i) {
    curr_prog[i] = tenants[i]->resrc_acct - prev_prog[i];
    total += curr_prog[i];
  }

  if (total.num_blks_done > 0) {  // some real progress is made
    cycles_per_block = total.cpu_consump / total.num_blks_done;
    measured_miss_rate = (total.num_blks_done != 0)
                             ? double(total.bw_consump) / total.num_blks_done
                             : std::numeric_limits<double>::infinity();
    if (measured_miss_rate != std::numeric_limits<double>::infinity() &&
        measured_miss_rate > 1.0) {
      SCHED_LOG_WARNING(
          "Measured miss rate is out-of range (should only happen if "
          "num_blk_done and bw_consump are very low): bw_consump=%ld, "
          "num_blks_done=%ld, measured_miss_rate=%lf",
          total.bw_consump, total.num_blks_done, measured_miss_rate);
      measured_miss_rate = 1.0;
    }
    uint64_t total_num_inodes = 0;
    HitRateCnt total_cache_stat{};
    if (!silent) {
      SCHED_LOG_NOTICE(HEADER0, aid, window);
      SCHED_LOG_NOTICE(HEADER1);
      SCHED_LOG_NOTICE(HEADER2);
      for (int i = 0; i < int(tenants.size()); ++i) {
        auto num_inodes = tenants[i]->app_proc->GetInos().size();
        auto a = tenants[i]->get_resrc();
        auto c = distr_ghost_cache_view.get_hit_rate_cnt(i, a.cache_size);

        Stat stat = {
            .p = curr_prog[i], .a = a, .c = c, .num_inodes = num_inodes};
        stat.print("W-" + std::to_string(i));

        total_cache_stat += c;
        total_num_inodes += num_inodes;
      }

      Stat stat = {.p = total,
                   .a = curr_resrc,
                   .c = total_cache_stat,
                   .num_inodes = total_num_inodes};
      stat.print("Sum");
    }
  } else {
    cycles_per_block = std::numeric_limits<int64_t>::max();
  }
  return total.num_blks_done > 0;
}
}  // namespace sched
