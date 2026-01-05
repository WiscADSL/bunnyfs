#pragma once

#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <iostream>
#include <limits>
#include <ostream>
#include <queue>

#include "BlockBufferItem.h"
#include "Log.h"
#include "Param.h"
#include "RateLimit.h"
#include "Resrc.h"
#include "Stat.h"
#include "gcache/ghost_cache.h"
#include "gcache/shared_cache.h"
#include "spdlog/spdlog.h"

class FsReq;
class BlockReq;
class AppProc;

class BlockBufferItem;

namespace gcache {

template <typename Tag_t, typename Key_t, typename Value_t, typename Hash>
class SharedCache;
}

namespace sched {

class Allocator;
class AppResrcView;

/**
 * Unlike AppProc, Tenant here is an encapsulation for scheduling.
 * Each tenant is a scheduling entity; it has its allocated share of each
 * resource and request queues.
 * Each tenant belongs to one worker, so read/write data structures inside does
 * not require locks; each app can have multiple tenant, each on different
 * workers. An app's total resources (across workers) is an instance of `class
 * AppResrcView`.
 */
class Tenant {
  AppProc *app_proc;
  // receive queue: requests from the client's shared memory
  std::queue<FsReq *> recv_queue;
  // internal ready queue: requests waiting for further process
  std::queue<FsReq *> intl_queue;
  // block queue: block requests waiting to be submitted
  std::queue<std::pair<BlockReq *, FsReq *>> blk_queue;

  // when sharing CPU, the server essentially do WFQ.
  // we divide the time into epoch, where each tenant's progress is 0 when an
  // epoch starts and grows when consuming cpu.
  // server always schedule the least-progress tenant that has work to do.
  // NOTE: we are not doing a strict start-time fair queueing (SFQ): SFQ is
  //       memoryless, so if a client is picked but it has no request, it will
  //       be considered as wasting its own time and its virtual time will
  //       proceed with the system virtual clock; however, many apps don't have
  //       a lot of queue depth, so it may cause such temporally idle. if we
  //       do SFQ, this may cause this app gets lower CPU share.
  uint64_t cpu_prog;

  ResrcAcct resrc_acct;  // resource consumption accounting
  ResrcCtrlBlock resrc_ctrl_block;
  uint32_t weight;

  // pointer to this tenant's LRU cache (for easy check whether this tenant has
  // unpopulated cache)
  const SharedCache_t::LRUCache_t *cache{nullptr};

  /**
   * If we try to do load-balancing for this tenant, we will need to export this
   * tenant's inodes and move them to other workers. To achieve so, we need to
   * first drain the inflight requests, so no inode will be pinned by this
   * tenant.
   */

  // a request flows:      shm -> recv_queue -> ??? -> intl_queue -> ??? -> shm
  // we count in-flight as this window:       [***************************]
  int num_reqs_inflight{0};
  bool is_drain{false};
  std::vector<std::tuple<int, int>> pending_inode_move{};

  // stat info
  sched::stat::LatencyStat block_latency_stat;

  friend Allocator;
  friend AppResrcView;

 public:
  // NOTE: cpu_share is currently unused...
  Tenant(int wid, int aid, AppProc *app_proc, uint32_t cache_size,
         int64_t bandwidth, int64_t cpu_cycles)
      : app_proc(app_proc),
        recv_queue(),
        intl_queue(),
        blk_queue(),
        cpu_prog(0),
        resrc_acct(),
        resrc_ctrl_block(cache_size, bandwidth, cpu_cycles),
        weight(std::max(params::cycles_to_weight(cpu_cycles),
                        params::min_weight)) {
    char buf[20];
    sprintf(buf, "W%d-A%d BIO", wid, aid);
    block_latency_stat.set_name(buf);
  }

  AppProc *get_app() const { return app_proc; }

  void set_cache(const SharedCache_t::LRUCache_t &c) { cache = &c; }

  std::string to_string() const;

  ~Tenant() { report(); }

  void report() const {
    std::stringstream report_buf;
    report_buf << "Total Read: "
               << params::blocks_to_mb(resrc_acct.num_blks_done) << "MB\n"
               << "Total I/O:  " << params::blocks_to_mb(resrc_acct.bw_consump)
               << " MB\n"
               << "Total CPU:  " << resrc_acct.cpu_consump << " cycles\n";
    report_buf << "Page Cache: "
               << params::blocks_to_mb(resrc_ctrl_block.curr_resrc.cache_size)
               << " MB\n"
               << "Bandwidth:  "
               << params::blocks_to_mb(resrc_ctrl_block.curr_resrc.bandwidth)
               << " MB/s\n"
               << "CPU Cost:   " << get_cpu_per_block() << " cycles/block\n";
    // resrc_ctrl_block.report_ghost_cache(report_buf);
    // std::cout << report_buf.str();
  }

  uint64_t get_cpu_prog() const { return cpu_prog; }
  void reset_cpu_prog() { cpu_prog = 0; }
  // cpu_prog is updated in `record_cpu_consump`

  ResrcAlloc get_resrc() const { return resrc_ctrl_block.curr_resrc; }
  void set_resrc(ResrcAlloc new_resrc) {
    weight = std::max(params::cycles_to_weight(new_resrc.cpu_cycles),
                      params::min_weight);
    resrc_ctrl_block.blk_rate_limiter.update_bandwidth(new_resrc.bandwidth);
    resrc_ctrl_block.curr_resrc = new_resrc;
    SCHED_LOG_NOTICE("Apply: cache=%d, bw=%ld, cpu=%ld", new_resrc.cache_size,
                     new_resrc.bandwidth, new_resrc.cpu_cycles);
  }

  // exposed to BlockBuffer LRU cache
  uint32_t get_max_cache_size() const {
    return std::max(resrc_ctrl_block.curr_resrc.cache_size, params::min_cache);
  }
  // weight is for CPU-only
  uint32_t get_weight() const { return weight; }

  // for allocator: the real, allocated weight
  uint32_t get_allocated_weight() const {
    return params::cycles_to_weight(resrc_ctrl_block.curr_resrc.cpu_cycles);
  }

  size_t get_recv_qlen() { return recv_queue.size(); }
  size_t get_intl_qlen() { return intl_queue.size(); }
  size_t get_blk_qlen() { return blk_queue.size(); }

  void add_recv_queue(FsReq *req) { recv_queue.emplace(req); }
  void add_intl_queue(FsReq *req) { intl_queue.emplace(req); }
  void add_blk_queue(BlockReq *blk_req, FsReq *req) {
    blk_queue.emplace(blk_req, req);
  }
  FsReq *pop_recv_queue() {
    if (recv_queue.empty() || is_drain) return nullptr;
    FsReq *req = recv_queue.front();
    recv_queue.pop();
    ++num_reqs_inflight;
    return req;
  }
  FsReq *pop_intl_queue() {
    if (intl_queue.empty()) return nullptr;
    FsReq *req = intl_queue.front();
    intl_queue.pop();
    return req;
  }
  BlockReq *pop_blk_queue(FsReq *&fs_req) {
    if (blk_queue.empty()) return nullptr;
    if (params::policy::cache_partition) {
      assert(cache);
      assert(cache->size() <= cache->capacity());
      if (params::policy::unlimited_bandwidth_if_unpopulated_cache) {
        // if the cache is not fully populated, we don't throttle this tenant's
        // bandwidth: it is likely that this tenant just gets extra cache space
        // and need to populate the cache to stabilize; we are sure that the
        // upper bound of bandwidth this tenant could consume without rate limit
        // is the unpopulated cache space
        if (!(cache->size() < cache->capacity())) {
          // only check rate limiter if cache is fully populated
          if (!resrc_ctrl_block.blk_rate_limiter.can_send()) return nullptr;
        }
      } else {  // always check rate limiter
        if (!resrc_ctrl_block.blk_rate_limiter.can_send()) return nullptr;
      }
    } else {  // always check rate limiter
      if (!resrc_ctrl_block.blk_rate_limiter.can_send()) return nullptr;
    }
    BlockReq *blk_req = blk_queue.front().first;
    fs_req = blk_queue.front().second;
    blk_queue.pop();
    // here we assume this block would be submitted to device immediately
    record_bw_consump(1);
    return blk_req;
  }

  // whether this tenant can be scheduled
  bool can_sched(uint64_t elapsed) {
    // if strict_cpu_usage is enabled, this tenant will be throttled if it has
    // consume more CPU cycles that it is allocated (this can ensure other
    // tenants get more responsive service)
    if (params::policy::strict_cpu_usage) {
      uint64_t consumed_cycles =
          params::progress_to_cycles(cpu_prog, get_weight());
      // `elapsed` here is the wall clock, not the worker available CPU time;
      // so we use `params::cycles_to_weight(params::cycles_per_second)` as
      // the denominator instead of `worker_avail_weight`
      uint64_t limited_cycles =
          elapsed * get_weight() /
          params::cycles_to_weight(params::cycles_per_second);
      if (consumed_cycles > limited_cycles) return false;
    }
    // check recv_queue and intl_queue must have something to schedule
    return !((recv_queue.empty() || is_drain) && (intl_queue.empty()));
  }

  void access_ghost_page(uint32_t page_id, bool is_write) {
    auto mode = is_write ? gcache::AccessMode::AS_MISS : gcache::AccessMode::DEFAULT;
    resrc_ctrl_block.ghost_cache.access(page_id, mode);
  }

  void record_blocks_done(uint32_t blocks) {
    resrc_acct.num_blks_done += blocks;
  }
  void record_cpu_consump(uint64_t cycles) {
    resrc_acct.cpu_consump += cycles;
    cpu_prog += params::cycles_to_progress(cycles, get_weight());
  }
  void record_bw_consump(uint32_t blocks) { resrc_acct.bw_consump += blocks; }
  void record_req_done() { --num_reqs_inflight; }

  uint64_t get_cpu_per_block() const {
    if (resrc_acct.num_blks_done == 0) return 0;
    return resrc_acct.cpu_consump / resrc_acct.num_blks_done;
  }
  void reset_stat() {
    resrc_acct.num_blks_done = 0;
    resrc_acct.cpu_consump = 0;
  }

  void turn_blk_rate_limiter(bool to_on) {
    resrc_ctrl_block.blk_rate_limiter.turn(to_on);
  }

  bool should_migrate() { return is_drain && num_reqs_inflight == 0; }

  void set_drain_for_migration(std::vector<std::tuple<int, int>> &&inode_move) {
    // the previous drain should be done
    assert(!is_drain && pending_inode_move.empty());
    is_drain = true;
    assert(pending_inode_move.empty());
    pending_inode_move = std::move(inode_move);
  }

  const std::vector<std::tuple<int, int>> &get_pending_inode_move() {
    assert(is_drain);
    return pending_inode_move;
  }

  void unset_drain_for_migration() {
    is_drain = false;
    pending_inode_move.clear();
  }

  void add_latency(uint64_t l) { block_latency_stat.add_latency(l); }
};

}  // namespace sched
