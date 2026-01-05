#pragma once

#include <fcntl.h>

#include <cassert>
#include <chrono>
#include <cstdint>
#include <stdexcept>
#include <vector>

#include "config.h"
#include "fsapi.h"
#include "offset.h"
#include "spec.h"
#include "thread.h"
#include "utils/logging.h"
#include "utils/stat.h"

inline std::string hexdump(const void* data, size_t size) {
  std::string result;
  result.reserve(size * 2);
  const auto* p = (const uint8_t*)data;
  for (size_t i = 0; i < size; i++) {
    result += fmt::format("{:02x}", p[i]);
    if (i % 4 == 3) result += " ";
    if (i % 32 == 31) result += "\n";
  }
  return result;
}

void check_data(const void* data, size_t count, size_t off) {
  static constexpr size_t len = 4096;
  static constexpr char zeros[len] = {};
  for (size_t j = 0; j < count; j += len) {
    if (memcmp((char*)data + j, zeros, len) != 0) {
      THREAD_ERROR("Read non-zero data at offset {}:\n{}", off + j,
                   hexdump((char*)data + j, len));
      throw std::runtime_error("Read non-zero data");
    }
  }
}

class Workload {
  const spec::Workload& spec;
  std::vector<void*> bufs;
  Offsets offsets;

 public:
  explicit Workload(const spec::Workload& spec)
      : spec(spec), offsets(spec.ops, spec.offset) {
    for (uint32_t i = 0; i < spec.qdepth; ++i)
      bufs.emplace_back(fs_zalloc(spec.count));
  }

  ~Workload() {
    for (uint32_t i = 0; i < spec.qdepth; ++i) fs_free(bufs[i]);
  }

  void run(const std::vector<int>& fds) {
    THREAD_DEBUG("Running workload {}: {}", spec.name, spec.dump());
    auto epoch_callback = [&](const Stat& stat) {
      const auto& info = stat.get_epoch_info();
      THREAD_INFO(
          "{}: Epoch {:2.0f}: {} ops in {:.2f} s ({:7.2f} MB/s, {:7.3f} us/op)",
          spec.name, stat.get_accum_info().get_elapsed_sec(), info.ops,
          info.get_elapsed_sec(), info.get_mbps(spec.count),
          info.get_latency_us_per_op());
    };
    Stat stat({.epoch_callback = epoch_callback}, spec.qdepth);

    std::unordered_map<int, Stat> per_fd_stat_map;
    std::unordered_map<int, uint64_t> per_fd_dirty_sizes;
    for (const auto& fd : fds) {
      auto final_callback = [&, fd](const Stat& stat) {
        const auto& info = stat.get_accum_info();
        THREAD_INFO("{} fd-{}: {:7.2f} MB/s, {:7.3f} us/op", spec.name, fd,
                    info.get_mbps(spec.count), info.get_latency_us_per_op());
      };
      per_fd_stat_map.emplace(
          std::piecewise_construct, std::forward_as_tuple(fd),
          std::forward_as_tuple(Stat::Args{.final_callback = final_callback},
                                1));
      per_fd_dirty_sizes.emplace(fd, 0);
    }

    size_t fd_idx = 0;
    size_t op_cnt = 0;
    size_t ctx_idx = 0;
    void* data = nullptr;
    async_ctx_rw* ctx = nullptr;
    int fd = 0;
    std::vector<async_ctx_rw> ctxs(spec.qdepth);
    std::vector<bool> ctxs_is_read(spec.qdepth);
    for (auto off : offsets) {
    begin:
      bool is_read = rand() % 100 < spec.read_ratio * 100;

      ssize_t rc;

      data = bufs[ctx_idx];
      ctx = &ctxs.data()[ctx_idx];
      fd = fds[fd_idx];

      if (spec.qdepth == 1) {  // use sync APIs
        assert(ctx_idx == 0);
        per_fd_stat_map.find(fd)->second.op_start();
        stat.op_start();

        if (is_read) {
          rc = fs_allocated_pread(fd, data, spec.count, off);
          if (rc != spec.count) {
            THREAD_ERROR(
                "{}: fs_allocated_pread returned {} on fd={}, count={}, off={}",
                spec.name, rc, fd, spec.count, off);
            throw std::runtime_error(
                "fs_allocated_pread does not return expected data");
          }
        } else {
          rc = fs_allocated_pwrite(fds[fd_idx], data, spec.count, off);
          if (rc != spec.count) {
            THREAD_ERROR(
                "{}: fs_allocated_pwrite returned {} on fd={}, count={}, "
                "off={}",
                spec.name, rc, fds[fd_idx], spec.count, off);
            throw std::runtime_error(
                "fs_allocated_pwrite does not return expected data");
          }
          uint64_t& fd_dirty = per_fd_dirty_sizes.find(fd)->second;
          fd_dirty += spec.count;
          if (fd_dirty >= spec.dirty_threshold) {
            int ret = fs_fdatasync(fds[fd_idx]);
            if (ret != 0) THREAD_ERROR("fdatasync returns {}", ret);
            fd_dirty = 0;
          }
        }
        stat.op_stop();
        per_fd_stat_map.find(fd)->second.op_stop();
      } else {
        if (op_cnt >= spec.qdepth) {
          // wait for the previous req to complete
          if (ctxs_is_read[ctx_idx]) {
            rc = fs_allocated_pread_wait(ctx);
            if (rc != spec.count) {
              THREAD_ERROR(
                  "{}: fs_allocated_pread_wait returned {} on fd={}, count={}, "
                  "off={}",
                  spec.name, rc, ctx->fd, spec.count, ctx->offset);
              throw std::runtime_error(
                  "fs_allocated_pread_wait does not return expected data");
            }
          } else {
            rc = fs_allocated_pwrite_wait(ctx);
            if (rc != spec.count) {
              THREAD_ERROR(
                  "{}: fs_allocated_pwrite_wait returned {} on fd={}, "
                  "count={}, off={}",
                  spec.name, rc, ctx->fd, spec.count, ctx->offset);
              throw std::runtime_error(
                  "fs_allocated_pwrite_wait does not write expected data");
            }
            uint64_t& fd_dirty = per_fd_dirty_sizes.find(ctx->fd)->second;
            fd_dirty += spec.count;
            if (fd_dirty >= spec.dirty_threshold) {
              int ret = fs_fdatasync(ctx->fd);
              if (ret != 0) THREAD_ERROR("fdatasync returns {}", ret);
              fd_dirty = 0;
            }
          }
          stat.op_stop(ctx_idx);
          per_fd_stat_map.find(ctx->fd)->second.op_stop();
        }
        per_fd_stat_map.find(fd)->second.op_start();
        stat.op_start(ctx_idx);
        if (is_read) {
          int submit_rc =
              fs_allocated_pread_submit(ctx, fd, data, spec.count, off);
          if (submit_rc) throw std::runtime_error("Fail to submit pread");
        } else {
          int submit_rc =
              fs_allocated_pwrite_submit(ctx, fd, data, spec.count, off);
          if (submit_rc) throw std::runtime_error("Fail to submit pwrite");
        }
        ctxs_is_read[ctx_idx] = is_read;
      }

      if (stat.get_accum_info().get_elapsed_sec() >= spec.duration_sec) break;

      if constexpr (config::DEBUG) {
        static constexpr size_t len = 4096;
        static constexpr char zeros[len] = {};
        for (size_t j = 0; j < spec.count; j += len) {
          if (memcmp((char*)data + j, zeros, len) != 0) {
            THREAD_ERROR("Read non-zero data at offset {}:\n{}", off + j,
                         hexdump((char*)data + j, len));
            throw std::runtime_error("Read non-zero data");
          }
        }

        if (rc != spec.count) {
          THREAD_ERROR("{}: Returned {} on fd={}, count={}, off={}", spec.name,
                       rc, fds[fd_idx], spec.count, off);
        }
      }

      if (stat.get_accum_info().get_elapsed_sec() >= spec.duration_sec) break;
      if constexpr (config::DEBUG) check_data(data, spec.count, off);
      ctx_idx = (ctx_idx + 1) % ctxs.size();
      fd_idx = (fd_idx + 1) % fds.size();
      op_cnt++;
      if (op_cnt > spec.ops) break;
      // For sequential workloads, we want to reuse the same offset
      // until all the files are used (when fd_idx == 0).
      if (spec.offset.type == spec::OffsetType::SEQ && fd_idx != 0) goto begin;
    }

    // clean up inflight requests
    if (spec.qdepth > 1) {
      auto ctx_cnt = std::min<size_t>(spec.qdepth, op_cnt);
      for (ctx_idx = 0; ctx_idx < ctx_cnt; ++ctx_idx) {
        ctx = &ctxs.data()[ctx_idx];
        if (ctxs_is_read[ctx_idx]) {
          ssize_t rc = fs_allocated_pread_wait(ctx);
          if (rc != spec.count) {
            THREAD_ERROR(
                "{}: fs_allocated_pread_wait returned {} on fd={}, count={}, "
                "off={}",
                spec.name, rc, ctx->fd, spec.count, ctx->offset);
            throw std::runtime_error(
                "fs_allocated_pread_wait does not return expected data");
          }
        } else {
          ssize_t rc = fs_allocated_pwrite_wait(ctx);
          if (rc != spec.count) {
            THREAD_ERROR(
                "{}: fs_allocated_pwrite_wait returned {} on fd={}, count={}, "
                "off={}",
                spec.name, rc, ctx->fd, spec.count, ctx->offset);
            throw std::runtime_error(
                "fs_allocated_pwrite_wait does not write expected data");
          }
        }
      }
    }
  }

  enum class DBWorkloadType { RW, SCAN };

  void run(leveldb::DB* db) {
    if (spec.count == 1)
      run_impl<DBWorkloadType::RW>(db);
    else
      run_impl<DBWorkloadType::SCAN>(db);
  }

  template <DBWorkloadType type>
  void run_impl(leveldb::DB* db) {
    THREAD_DEBUG("Running workload {}: {}", spec.name, spec.dump());
    auto epoch_callback = [&](const Stat& stat) {
      const auto& info = stat.get_epoch_info();
      THREAD_INFO(
          "{}: Epoch {:2.0f}: {} ops in {:.2f} s ({:7.2f} kops, {:7.3f} "
          "us/op)",
          spec.name, stat.get_accum_info().get_elapsed_sec(), info.ops,
          info.get_elapsed_sec(), info.get_iops() / 1000,
          info.get_latency_us_per_op());
    };
    Stat stat({.epoch_callback = epoch_callback}, 1);

    leveldb::WriteOptions write_options;
    write_options.sync = true;

    leveldb::ReadOptions read_options;
    //    read_options.fill_cache = false;  // Don't fill cache

    leveldb::Status status;
    leveldb::Iterator* it = db->NewIterator(read_options);
    std::string value;
    std::string write_value = std::string(100, 'a');
    for (auto off : offsets) {
      std::string key = get_key_from_idx(off);
      stat.op_start();
      if constexpr (type == DBWorkloadType::RW) {
        bool is_read = rand() % 100 < spec.read_ratio * 100;
        if (is_read)
          status = db->Get(read_options, key, &value);
        else
          status = db->Put(write_options, key, value);
      } else if constexpr (type == DBWorkloadType::SCAN) {
        it->Seek(key);
        for (size_t count = 0; count < spec.count; count++) {
          if (!it->Valid()) break;
          value = it->value().ToString();
          it->Next();
        }
        status = it->status();
      }
      stat.op_stop();
      if (stat.get_accum_info().get_elapsed_sec() >= spec.duration_sec) break;
      if (!status.ok()) {
        SPDLOG_ERROR("{} failed to get key \"{}\": {}", spec.name, key,
                     status.ToString());
        throw std::runtime_error("Failed to get key");
      }
    }
  }
};
