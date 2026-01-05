#ifndef CFS_BLOCKBUFFER_H
#define CFS_BLOCKBUFFER_H

#include <atomic>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <iostream>
#include <list>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "BlockBufferFlusher.h"
#include "BlockBufferItem.h"
#include "Param.h"
#include "Tag.h"
#include "Tenant.h"
#include "gcache/shared_cache.h"
#include "spdlog/spdlog.h"
#include "typedefs.h"
#include "util.h"

class BlockBuffer {
 public:
#ifdef DO_SCHED
  BlockBuffer(const std::vector<std::pair<sched::Tag, size_t>> &config,
              int blockSize, char *memPtr, bool isReportStat = false,
              const std::string &name = {})
      : capacity([&]() {
          size_t result = 0;
          for (auto [t, c] : config) result += c;
          return result;
        }()),
        blockSize(blockSize),
        isMultiTenantSupported(true),
        bufferName(name),
        flusher(config) {
    auto setBufPtr = [&, i = 0l](BlockBufferHandle item) mutable {
      item->init(this, memPtr + (i++) * blockSize);
    };
    lruCache.init(config, setBufPtr);
    for (auto [t, c] : config) {
      sched::Tenant *tenant = t.get_tenant();
      if (tenant) tenant->set_cache(lruCache.get_cache(t));
    }
  }
#endif

  BlockBuffer(block_no_t blockNum, int blockSize, char *memPtr,
              bool isReportStat = false, const std::string &name = {})
      : capacity(blockNum),
        blockSize(blockSize),
        isMultiTenantSupported(false),
        bufferName(name),
        flusher({{sched::tag::unalloc, blockNum}}) {
    assert(blockNum <= std::numeric_limits<block_no_t>::max());
    auto setBufPtr = [&, i = 0l](BlockBufferHandle item) mutable {
      item->init(this, memPtr + (i++) * blockSize);
    };
    lruCache.init({{sched::tag::unalloc, blockNum}}, setBufPtr);
  }

  ~BlockBuffer() = default;

  // get a buffer handle for blockNo
  // @return nullptr when buffer is full and cannot insert new item
  BlockBufferHandle getBlock(block_no_t blockNo, uint32_t new_index = 0,
                             sched::Tenant *tenant = nullptr,
                             bool isWrite = false) {
    // NOTE: tenant can be nullptr for shared data structures, e.g. inode
    if (!isMultiTenantSupported) tenant = nullptr;

    BlockBufferHandle item = lruCache.lookup(blockNo, true);
    // a item can:
    // 1) in_mem -> ready to read
    // 2) !in_mem & pending_blk_req -> has been submitted to device
    // 3) !in_mem & !pending_blk_req -> need to submit
    if (item) {  // cache hit
#ifdef DO_SCHED
      // if a block access is a miss, it will be submitted to the device and
      // be called `getBlock` again when the data is ready. thus, we ignore
      // the first access and only count the case of a hit.
      if (tenant) {
        tenant->access_ghost_page(blockNo, isWrite);  // maintain ghost cache
        tenant->record_blocks_done(1);
      }
#endif
    } else {
      // tenant can be nullptr if !isMultiTenantSupported
      sched::Tag tag = tenant ? (sched::params::policy::cache_partition
                                     ? sched::Tag{.tenant = tenant}
                                     : sched::tag::global)
                              : (sched::tag::unalloc);
      if (isWrite && tenant) tenant->record_blocks_done(1);
      item = lruCache.insert(tag, blockNo, /*pin*/ true,
                             /*hint_nonexist*/ true);
      if (item == nullptr) return nullptr;
      item->reset();
      auto orig_index = item->getIndex();
      // NOTE: we never put 0 into the blockIndexMap
      // 0 is never a valid ino
      if (orig_index != new_index) {
        // If LRU cache replacement happens, the return one
        if (orig_index) blockIndexMap[orig_index].erase(item);
        if (new_index) blockIndexMap[new_index].emplace(item);
        item->setIndex(new_index);
      }
    }

    assert(!new_index || item->getIndex() == new_index);
    // if new_index is 0, it will be reset back to 0
    // otherwise, just set it
    return item;
  }

  template <typename Fn>
  void forEachBlock(Fn fn) {
    lruCache.for_each(fn);
  }

  void releaseBlock(BlockBufferHandle item) {
    SPDLOG_DEBUG("BlockBuffer::releaseBlock: blockNo {} index {} refs {} in {}",
                 item.get_key(), item->getIndex(), bufferName);
    return lruCache.release(item);
  }

  // About dirty block flush
  // Set the item in this BlockBuffer as dirty, will at the same time do the
  // accounting of dirty blocks.
  // @return: original isDirty value of this block
  void setBlockDirty(BlockBufferHandle item, uint32_t itemIndex) {
    bool used_to_be_dirty = item->setDirty(true);
    if (!used_to_be_dirty) {
      flusher.addDirtyItem(item, itemIndex);
      lruCache.pin(item);
    }
  }

  void unsetBlockDirty(BlockBufferHandle item) {
    bool used_to_be_dirty = item->setDirty(false);
    if (used_to_be_dirty) {
      flusher.removeDirtyItem(item);
      lruCache.release(item);
    }
  }

  void releaseUnlinkedInodeDirtyBlocks(uint32_t itemIndex) {
    assert(itemIndex > 1);  // cannot unlink rootino(1)
    auto all_it = blockIndexMap.find(itemIndex);
    int num_actual_dirty_blocks = 0;
    if (all_it != blockIndexMap.end()) {
      // SPDLOG_INFO("{} releaseBuffer index:{} num:{}", bufferName, itemIndex,
      //             it->second.size());
      auto t = all_it->second;

      for (auto handle : all_it->second) {
        bool orig = handle->setDirty(false);
        if (orig) {
          num_actual_dirty_blocks++;
        }
        lruCache.release(handle);
      }
      blockIndexMap.erase(all_it);
    }

    int num_dirty_blocks = flusher.removeDirtyItemByIndex(itemIndex);
    assert(num_dirty_blocks == num_actual_dirty_blocks);

    auto it2 = blockIndexMap.find(itemIndex);
    if (it2 != blockIndexMap.end()) blockIndexMap.erase(it2);
  }

  // split the BufferItems that associated to the index
  // @ param items: the resulting bufferItems will be put into items
  // @ return: error will be -1, ok --> 0
  int splitBufferItemsByIndex(uint32_t index,
                              std::vector<ExportedBlockBufferItem> &itemSet);

  // install buffer slot in into this buffer
  void installBufferItemsOfIndex(
      uint32_t index, const std::vector<ExportedBlockBufferItem> &itemSet,
      const std::unordered_map<pid_t, AppProc *> &appMap);

  int getCurrentItemNum() {
    int num = 0;
    lruCache.for_each([&](block_no_t bno, BlockBufferHandle item) { num++; });
    return num;
  }

  void adjustCacheSize(sched::Tag t) {
#ifdef DO_SCHED
    assert(sched::params::policy::cache_partition);
    sched::Tenant *tenant = t.get_tenant();
    assert(tenant);
    auto old_size = lruCache.capacity_of(t);
    auto new_size = tenant->get_max_cache_size();
    SCHED_LOG_NOTICE("Adjust cache size: %ld -> %d", old_size, new_size);
    if (old_size < new_size) {
      auto move_cnt = new_size - old_size;
      auto done_cnt = lruCache.relocate(/*src*/ sched::tag::unalloc,
                                        /*dst*/ t, move_cnt);
      if (done_cnt != move_cnt) {
        SCHED_LOG_WARNING("Expect to give %ld; successfully give %ld", move_cnt,
                          done_cnt);
        SPDLOG_WARN(
            "Adjust cache size: {} -> {}; "
            "Expect to give {}; successfully give {}",
            old_size, new_size, move_cnt, done_cnt);
      }
    } else if (old_size > new_size) {
      auto move_cnt = old_size - new_size;
      auto done_cnt = lruCache.relocate(
          /*src*/ t, /*dst*/ sched::tag::unalloc, move_cnt);
      if (done_cnt != move_cnt) {
        SCHED_LOG_WARNING("Expect to take %ld; successfully take %ld", move_cnt,
                          done_cnt);
        SPDLOG_WARN(
            "Adjust cache size: {} -> {}; "
            "Expect to take {}; successfully take {}",
            old_size, new_size, move_cnt, done_cnt);
      }
    }
#endif
  }

 private:
  /**
   * SharedCache use tag to distinguish tenant. Each tenant will have a unique
   * tag (in our implementation, it is Tenant pointer). nullptr is a special
   * type of tag: in DO_SCHED mode, it means unused cache slots; in NOOP_SCHED
   * (!DO_SCHED) mode, all cache slots are under this tag.
   */
  SharedCache_t lruCache;

  /**
   * This maps a inode number (i.e. index) to a set of cache slots that holds
   * the data of this file. This map is actively updated when lruCache is
   * queried.
   */
  std::unordered_map<uint32_t, std::unordered_set<BlockBufferHandle>>
      blockIndexMap;

 public:
  // capacity of the buffer in number of blocks
  const block_no_t capacity;

  // block size in bytes
  const int blockSize;

  const bool isMultiTenantSupported;

  const std::string bufferName;
  blockbuffer::Flusher flusher;

  friend std::ostream &operator<<(std::ostream &os, const BlockBuffer &buffer) {
    os << "lruCache: " << buffer.lruCache;
    return os;
  }
};

#endif  // CFS_BLOCKBUFFER_H
