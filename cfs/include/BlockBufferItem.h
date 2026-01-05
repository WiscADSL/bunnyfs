#ifndef CFS_BLOCKBUFFERITEM_H
#define CFS_BLOCKBUFFERITEM_H

#include <cassert>
#include <cstdint>
#include <cstddef>
#include <iostream>

#include "gcache/ghost_cache.h"
#include "gcache/shared_cache.h"
#include "Tag.h"
#include "typedefs.h"
#include "util.h"

class BlockBuffer;
class BlockReq;

namespace sched {
class Tenant;
}

class BlockBufferItem {
  /**
   * BlockBufferItem is a value indexed by block number. The member fields have
   * very different lifecycles:
   * - pool: is set only 1) at init time; 2) after migration.
   * - ptr: is set only at init time; if LRU replacement happens, the same
   *     BlockBufferItem will be reused by another block number, but this ptr
   *     remains the same (i.e., the cache slot binding to this item now belongs
   *     to a new block).
   * - index: is actively maintained after each `getBlock` (including potential
   *     LRU replacement). this field indicates the inode number (aka. index) of
   *     this item. Maybe zero if not the block is not a file data block.
   * - isBufDirty & isMem: reset after LRU; set when necessary.
   * - pending_blk_req: is set if a LRU happens and a block request is sent to
   *     the device; unset once I/O is done (inMem is set)
   */
 public:
  BlockBufferItem() = default;
  BlockBufferItem(const BlockBufferItem &) = default;

  [[nodiscard]] BlockBuffer *getPool() const { return pool; }
  [[nodiscard]] char *getBufPtr() const { return ptr; }
  [[nodiscard]] uint32_t getIndex() const { return index; }
  [[nodiscard]] bool isDirty() const { return isBufDirty; }
  [[nodiscard]] bool isInMem() const { return inMem; }
  [[nodiscard]] BlockReq *getPendingBlkReq() const { return pendingBlockReq; }

  void init(BlockBuffer *buf_pool, char *buf_ptr) {
    reset();
    pool = buf_pool;
    ptr = buf_ptr;
    index = 0;
  }

  void setIndex(uint32_t idx) { index = idx; }

  // blockNo is available from handle.get_key()
  // tenant* is available from handle.get_tag()

  void reset() {
    inMem = false;
    isBufDirty = false;
    pendingBlockReq = nullptr;
  }

  // state transition: if LRU happens, the new block data must be fetched from
  // the storage device. in this case, a pending block request is attached.
  void set_IO_submitted(BlockReq *r) {
    assert(!inMem);
    pendingBlockReq = r;
  }

  // state transition: if I/O completes, the data is in-memory.
  void set_IO_done() {
    assert(!inMem);
    inMem = true;
    pendingBlockReq = nullptr;
  }

  bool setDirty(bool d) { return std::exchange(isBufDirty, d); }

 private:
  // unfortunately, we need this when returning it back to the pool
  BlockBuffer *pool = nullptr;
  char *ptr = nullptr;
  uint32_t index = 0;
  bool isBufDirty = false;
  bool inMem = false;
  // if a block is not in memory and has been submitted to SSD for reading, this
  // field will be set
  BlockReq *pendingBlockReq = nullptr;

 public:
  bool operator==(const BlockBufferItem &other) const {
    return ptr == other.ptr;
  }

  bool operator!=(const BlockBufferItem &other) const {
    return ptr != other.ptr;
  }

  friend std::ostream &operator<<(std::ostream &os,
                                  const BlockBufferItem &item) {
    os << (void *)item.ptr << "(idx=" << item.index
       << ", dirty=" << item.isBufDirty << ", inMem=" << item.inMem << ")";
    return os;
  }

  // friend class BlockBuffer;
};

using SharedCache_t =
    gcache::SharedCache</*Tag_t*/ sched::Tag, /*Key_t*/ block_no_t,
                        /*Value_t*/ BlockBufferItem, /*Hash*/ gcache::ghash>;
using BlockBufferHandle = SharedCache_t::Handle_t;

struct ExportedBlockBufferItem {
  char *ptr;
  block_no_t block_no;
  bool is_dirty;
#ifdef DO_SCHED
  int aid;  // translated into `Tenant*` as SharedCache tag
#endif
  // other fields are known

  ExportedBlockBufferItem(BlockBufferHandle h);
};

// Implement hash function for BlockBufferItem
namespace std {
template <>
struct hash<BlockBufferItem> {
  std::size_t operator()(const BlockBufferItem &item) const {
    return std::hash<char *>{}(item.getBufPtr());
  }
};
template <>
struct hash<BlockBufferHandle> {
  std::size_t operator()(const BlockBufferHandle &item) const {
    return std::hash<char *>{}(item->getBufPtr());
  }
};
}  // namespace std

#endif  // CFS_BLOCKBUFFERITEM_H
