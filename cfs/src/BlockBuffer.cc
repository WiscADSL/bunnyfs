#include "BlockBuffer.h"

#include "FsProc_App.h"
#include "Tag.h"
#include "Tenant.h"

// This function will try to keep cache size constant
int BlockBuffer::splitBufferItemsByIndex(
    uint32_t index, std::vector<ExportedBlockBufferItem> &itemSet) {
  auto it = blockIndexMap.find(index);
  if (it == blockIndexMap.end()) return -1;

  // remove from cache
  for (auto handle : /*set of handles associated with index*/ it->second) {
    // SCHED_LOG_DEBUG("Export BufferItem: block-%d (index-%d)",
    //                 handle.get_key(), index);
    itemSet.emplace_back(handle);
    if (handle->isDirty()) {
      // No need to remove dirty item from flusher, since we will remove all
      // dirty items of this index
      //      flusher.removeDirtyItem(handle);
      lruCache.release(handle);
    }
    // ExportedBlockBufferItem ctor will do conversion
    bool export_success = lruCache.erase(handle);
    if (!export_success)
      throw std::runtime_error("Fail to export inode: a block is pinned!");
#ifdef DO_SCHED
    // since export will cause cache capacity--, add one cache entry back
    sched::Tag t = handle.get_tag();
    auto move_cnt =
        lruCache.relocate(/*src*/ sched::tag::unalloc, /*dst*/ t, 1);
    if (move_cnt != 1) SCHED_LOG_WARNING("Fail to add cache slot after export");
#endif
  }

  blockIndexMap.erase(it);
  flusher.removeDirtyItemByIndex(index);
  return 0;
}

// This function will try to keep cache size constant
void BlockBuffer::installBufferItemsOfIndex(
    uint32_t index, const std::vector<ExportedBlockBufferItem> &itemSet,
    const std::unordered_map<pid_t, AppProc *> &appMap) {
  if (itemSet.empty()) return;
  auto &curSet = blockIndexMap[index];
  for (auto &item : itemSet) {
#ifdef DO_SCHED
    sched::Tag t = {.tenant = &appMap.find(item.aid)->second->getTenant()};
#else
    sched::Tag t = sched::tag::unalloc;
#endif
    auto curBlockNo = item.block_no;
    auto handle = lruCache.install(t, curBlockNo);
    handle->init(this, item.ptr);
    handle->setIndex(index);
    // the handle is equivalent to just have I/O done: it is in-memory and no
    // pending block request
    handle->set_IO_done();
    if (item.is_dirty) {
      handle->setDirty(true);
      flusher.addDirtyItem(handle, index);
      lruCache.pin(handle);
    }
    curSet.emplace(handle);
#ifdef DO_SCHED
    // since import will cause capacity++; take one cache entry away
    auto move_cnt =
        lruCache.relocate(/*src*/ t, /*dst*/ sched::tag::unalloc, 1);
    if (move_cnt != 1)
      SCHED_LOG_WARNING("Fail to reduce cache slot after import");
#endif
    // SCHED_LOG_DEBUG("Import BufferItem: block-%d (index-%d)",
    // handle.get_key(),
    //                 index);
  }
  return;
}
