#include "Tenant.h"

#include "FsProc_App.h"
#include "FsProc_Fs.h"
#include "spdlog/spdlog.h"

namespace sched {

std::string Tenant::to_string() const {
  return fmt::format(
      "App{} on W{}: "
      "{:7.3f} GB RW, {:6.3f} GB BW, {:6.3f} G cycles | "
      "{:3} MB cache, {:4} MB/s BW, {:5} cycles/blk",
      app_proc->getAid(), app_proc->getWorker()->getWid(),
      params::blocks_to_mb(resrc_acct.num_blks_done) / 1024.0,
      params::blocks_to_mb(resrc_acct.bw_consump) / 1024.0,
      resrc_acct.cpu_consump / 1e9,
      params::blocks_to_mb(resrc_ctrl_block.curr_resrc.cache_size),
      params::blocks_to_mb(resrc_ctrl_block.curr_resrc.bandwidth),
      get_cpu_per_block());
  //   resrc_ctrl_block.report_ghost_cache(report_buf);
};


}  // namespace sched
