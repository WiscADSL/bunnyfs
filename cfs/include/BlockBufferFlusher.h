#ifndef CFS_BLOCKBUFFERFLUSHER_H
#define CFS_BLOCKBUFFERFLUSHER_H

#include <cstdint>
#include <cstdio>
#include <list>
#include <map>
#include <unordered_set>
#include <utility>

#include "BlockBufferItem.h"
#include "Tag.h"
#include "spdlog/spdlog.h"
#include "util.h"

namespace blockbuffer {

struct TenantInfo {
  sched::Tag tag = sched::tag::unalloc;
  size_t capacity = 0;
  size_t numDirty = 0;

  [[nodiscard]] double getDirtyRatio() const {
    return static_cast<double>(numDirty) / capacity;
  }

  [[nodiscard]] bool isAboveThreshold(double threshold) const {
    const double dirtyRatio = getDirtyRatio();
    if (dirtyRatio > threshold) {
      SPDLOG_DEBUG("Dirty blocks for tenant {} is above threshold: {}/{} > {}",
                   fmt::ptr(tag.get_tenant()), numDirty, capacity, threshold);
      return true;
    }
    return false;
  }

  friend std::ostream &operator<<(std::ostream &os, const TenantInfo &ti) {
    return os << "{{tag=" << ti.tag << ", capacity=" << ti.capacity
              << ", numDirty=" << ti.numDirty << "}}";
  }
};

class Flusher {
 public:
  Flusher(const std::vector<std::pair<sched::Tag, size_t>> &config) {
    for (auto [t, c] : config) tenantInfoMap.emplace(t, TenantInfo{t, c});
  }
  Flusher(const Flusher &) = delete;
  Flusher &operator=(const Flusher &) = delete;

  bool checkIfIdxFgFlushInflight(int index) {
    return fgIndices.find(index) != fgIndices.end();
  }

  // Check if the blockBuffer needs to do a background flush to avoid using up
  // of block slots.
  bool checkIfNeedBgFlush() {
    if (bgFlushSent) {
      // if there is BG flushing task not done
      return false;
    }
    if (checkIfFgFlushInflight() > 0 || (!fgIndices.empty())) {
      // if there is fore-ground flushing going on
      // or there are FsReq needs to do fore-ground flushing that is waiting
      return false;
    }

    for (auto [t, c] : tenantInfoMap)
      if (c.isAboveThreshold(dirtyRatioThreshold)) return true;
    return false;
  }

  // FgFlush: foreground flushing -- flushing that is in critical IO path

  void removeFgFlushWaitIndex(uint32_t idx) {
    SPDLOG_DEBUG("remove idx:{}", idx);
    if (fgIndices.find(idx) != fgIndices.end()) {
      fgIndices.erase(idx);
    } else {
      SPDLOG_DEBUG("removeFlushWaitIndex cannot find index. idx:{}", idx);
    }
  }

  // TODO (jingliu): to improve the performance of flushing.
  // Need to find the dirty blocks of this inode quickly
  // BlockBuffer::blockIndexMap_ can be used in this case, iterate all blocks
  // belongs to that inode, and then see if it is dirty
  // if index == 0, it is regarded to ignore index and flush several dirty
  // blocks
  void doFlushByIndex(uint32_t index, bool &canFlush,
                      std::list<BlockBufferHandle> &toFlushBlockNos) {
    // find blocks to be flushed
    uint32_t bnum = 0;
    canFlush = true;
    if (checkIfFgFlushReachLimit()) {
      canFlush = false;
    } else {
      if (index == 0) {
        // background flush
        for (auto &idxSetPair : dirtyIndexMap) {
          for (auto item : idxSetPair.second) {
            assert(item->isDirty());
            toFlushBlockNos.push_back(item);
            bnum++;
            if (bnum >= dirtyFlushOneTimeSubmitNum) {
              goto BREAK_OUT;
            }
          }
        }
      BREAK_OUT:;
      } else {
        // fsync to specific inode
        auto it = dirtyIndexMap.find(index);
        SPDLOG_DEBUG("doFlushByIndex index:{}", index);
        // NOTE: here, for a new created file, the inode won't have entry
        // in this map, since none data block has been added to that inode
        if (it != dirtyIndexMap.end()) {
          for (auto item : it->second) {
            if (item->isDirty()) {
              toFlushBlockNos.push_back(item);
              bnum++;
            } else {
              SPDLOG_ERROR(
                  "error flushByIndex index:{} bno:{} itemIndex:{} not dirty",
                  index, item.get_key(), item->getIndex());
            }
          }
        }
      }
      if (index == 0 && (!toFlushBlockNos.empty())) bgFlushSent = true;
    }
    SPDLOG_DEBUG("doFlushByIndex index:{} bnum:{}", index, bnum);
  }

  // fill a list of blocks that needs to be flushed
  // will set canFlush as the FLAG for the caller to see if flush request
  // will be issued or not this time. Because at any time, there should be only
  // one inflight flush request.
  // @param canFlush: if the buffer can be flushed, will be set to true
  // @param toFlushBlockNos: save the block numbers to be flushed
  void doFlush(bool &canFlush, std::list<BlockBufferHandle> &toFlushBlockNos) {
    doFlushByIndex(0, canFlush, toFlushBlockNos);
  }

  int doFlushDone() {
    if (!(bgFlushSent || checkIfFgFlushInflight())) {
      SPDLOG_ERROR(
          "doFlushDone called but flushSent is false bgSent:{} FGNum:{}",
          bgFlushSent, numFgFlush);
      return -1;
    }

    bgFlushSent = false;
    return 0;
  }

  void addDirtyItem(BlockBufferHandle item, uint32_t itemIndex) {
    dirtyIndexMap[itemIndex].emplace(item);
    tenantInfoMap[item.get_tag()].numDirty++;
    SPDLOG_DEBUG("addDirtyItem item:{}, idx={}, curr={}", item.get_key(),
                 itemIndex, tenantInfoMap[item.get_tag()].numDirty);
  }

  void removeDirtyItem(BlockBufferHandle item) {
    uint32_t itemIndex = item->getIndex();
    int num_removed = dirtyIndexMap[itemIndex].erase(item);
    // this should be 1; removing a non-existing index does not make sense
    assert(num_removed == 1);
    if (dirtyIndexMap[itemIndex].empty()) dirtyIndexMap.erase(itemIndex);
    tenantInfoMap[item.get_tag()].numDirty -= num_removed;
    SPDLOG_DEBUG("removeDirtyItem item:{}, idx={}, curr={}", item.get_key(),
                 itemIndex, tenantInfoMap[item.get_tag()].numDirty);
  }

  int removeDirtyItemByIndex(uint32_t itemIndex) {
    auto it = dirtyIndexMap.find(itemIndex);
    if (it == dirtyIndexMap.end()) return 0;
    int num = it->second.size();
    assert(num > 0);  // should not be empty, otherwise, why it is in the map?
    sched::Tag t = it->second.begin()->get_tag();
    tenantInfoMap[t].numDirty -= num;
    dirtyIndexMap.erase(itemIndex);
    SPDLOG_DEBUG("removeDirtyItemByIndex itemIndex:{}, curr={}", itemIndex,
                 tenantInfoMap[t].numDirty);
    return num;
  }

  void addFgFlushWaitIndex(uint32_t idx) {
    SPDLOG_DEBUG("addFlushWaitIdx:{}", idx);
    fgIndices.emplace(idx);
  }

  void setDirtyRatio(float r) {
    assert(r >= 0 and r <= 1);
    dirtyRatioThreshold = r;
  }
  bool checkIfFgFlushReachLimit() const { return numFgFlush >= fgFlushLimit; }
  bool checkIfFgFlushInflight() const { return numFgFlush > 0; }
  void addFgFlushInflightNum(int i) { numFgFlush += i; }

  void setDirtyFlushOneTimeSubmitNum(uint32_t n) {
    dirtyFlushOneTimeSubmitNum = n;
  }

  void setFgFlushLimit(int n) { fgFlushLimit = n; }

  ssize_t getDirtyItemNum() const {
    ssize_t num = 0;
    for (auto &idxSetPair : dirtyIndexMap) {
      num += idxSetPair.second.size();
    }
    return num;
  }

  // By default, we only allow 10 inflight fore-ground syncing
  constexpr static int kNumFgFlushLimit = 10;

 private:
  // map tenant tag to its' info. If !DO_SCHED, all info is under key
  // `sched::tag::unalloc`; if DO_SCHED && sched::params::cache_partition, there
  // will be no individual tenant tag but `sched::tag::global`
  std::unordered_map<sched::Tag, TenantInfo> tenantInfoMap;

  // the number of fore-ground flush inflight
  int numFgFlush = 0;

  int fgFlushLimit = kNumFgFlushLimit;

  // at any given time, we allow only max=1 back ground flushing in-flight to
  // device
  bool bgFlushSent = false;

  // when dirty blocks is over this ratio, will do the flushing.
  // by default we do not actively do flushing at all.
  float dirtyRatioThreshold = 1;

  // Control how many blocks will be flushed once checkIfNeedFlush() is true
  uint32_t dirtyFlushOneTimeSubmitNum = 0;

  // used to track if there are foreground *sync* request from Apps
  // e.g., (fsync(ino)), which ino is regarded is index
  // NOTE, only store the FsReq that does not issue *sync*'s flushing
  // It is not able to be flushed, but rejected by *checkIfReachFgFlushLimit*
  // We give the foreground fsync priority, so once this waitForFlushIndexes
  // is not empty, background flushing will be paused
  std::unordered_set<uint32_t> fgIndices;

  std::unordered_map<uint32_t, std::unordered_set<BlockBufferHandle>>
      dirtyIndexMap;

  friend std::ostream &operator<<(std::ostream &os, const Flusher &f) {
    os << "Flusher: dirtyRatio:" << f.dirtyRatioThreshold
       << " maxSubmitNum:" << f.dirtyFlushOneTimeSubmitNum
       << " fgFlushLimit:" << f.fgFlushLimit << " numFgFlush:" << f.numFgFlush
       << " bgFlushSent:" << f.bgFlushSent << " dirtyIndexMap:";
    for (auto &item : f.dirtyIndexMap)
      os << "{index:" << item.first << " dirtyNum:" << item.second.size()
         << "},";
    for (auto &item : f.tenantInfoMap) os << item.second << ",";
    return os;
  }
};

}  // namespace blockbuffer

#endif  // CFS_BLOCKBUFFERFLUSHER_H
