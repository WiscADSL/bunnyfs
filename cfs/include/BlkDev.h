#ifndef CFS_BLKDEV_H
#define CFS_BLKDEV_H

#include <atomic>
#include <string>

#include "typedefs.h"

// TODO: support bulk read/write request convering multiple blocks
enum class BlkDevReqType {
  BLK_DEV_REQ_DEFAULT,
  BLK_DEV_REQ_READ,
  BLK_DEV_REQ_WRITE,
  BLK_DEV_REQ_SECTOR_READ,
  BLK_DEV_REQ_SECTOR_WRITE,
};

struct BdevIoContext {
  char *buf;
  // optionally, the caller could embed a ptr as a payload here; the exact
  // purpose of this payload is defined by the caller as long as; make sure only
  // read payload if write it first. currently used by read req for embedding
  // BlockkReq*
  void *ctx_payload;
  uint64_t blockNo;
  uint64_t blockNoSeqNo;
  BlkDevReqType reqType;
  cfs_tid_t tid;
  bdev_reqid_t rid;
  // Used when busy checking the status of this request, e.g. blockingRead()
  bool isDone;  // no atomic needed...
  BdevIoContext()
      : buf(nullptr),
        blockNo(0),
        blockNoSeqNo(0),
        reqType(BlkDevReqType::BLK_DEV_REQ_DEFAULT),
        tid(0),
        rid(0),
        isDone(false) {}
};

class BlkDev {
 public:
  BlkDev(const std::string &path, uint32_t blockNum, uint32_t blockSize)
      : devPath(path), devBlockNum(blockNum), devBlockSize(blockSize) {}
  virtual ~BlkDev(void){};
  virtual int devInit() = 0;
  virtual int read(uint64_t blockNo, char *data, void* ctx_payload) = 0;
  virtual int write(uint64_t blockNo, uint64_t blockNoSeqNo, char *data) = 0;
  virtual void *zmallocBuf(uint64_t size, uint64_t align) = 0;
  virtual int freeBuf(void *ptr) = 0;
  virtual int devExit(void) = 0;

 protected:
  std::string devPath;
  uint32_t devBlockNum;
  uint32_t devBlockSize;
};

#endif  // CFS_BLKDEV_H
