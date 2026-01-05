#include "BlockBuffer.h"
#include "gtest/gtest.h"

// NOTE: REQUIRED to run with valgrind for memory leak
// valgrind --leak-check=yes ${BIN}

using namespace blockbuffer;

TEST(LRUTest, Test) {
  gcache::LRUCache<int, int, gcache::ghash> cache;
  cache.init(1);
  auto handle1 = cache.insert(1, true);
  EXPECT_NE(handle1, nullptr);

  EXPECT_EQ(cache.lookup(2, true), nullptr);
  EXPECT_EQ(cache.insert(2, true), nullptr);

  cache.release(handle1);
}

namespace {
class BlockBufferSplitTest : public ::testing::Test {
 protected:
  static constexpr uint32_t blockNum = 4;
  static constexpr int blockSize = 32;
  char memPtr[blockNum * blockSize]{};
  BlockBuffer buffer{blockNum, blockSize, memPtr};
};

TEST_F(BlockBufferSplitTest, SingleBlock) {
  const int index = 1;

  // Split a single block
  {
    BlockBufferHandle item = buffer.getBlock(1000, index);
    EXPECT_NE(item, nullptr);
    EXPECT_EQ(item->getIndex(), index);
    std::vector<ExportedBlockBufferItem> itemSet;
    buffer.splitBufferItemsByIndex(index, itemSet);
    EXPECT_EQ(itemSet.size(), 1);
  }

  std::vector<BlockBufferHandle> handles;
  for (uint i = 0; i < blockNum; i++) {
    BlockBufferHandle item = buffer.getBlock(1001 + i, index);
    EXPECT_NE(item, nullptr);
    // NOTE, we do not release block here
    handles.push_back(item);
  }

  // the buffer should be full
  EXPECT_EQ(buffer.getBlock(1001 + blockNum, index), nullptr);

  // and 1000 should have been evicted
  EXPECT_EQ(buffer.getBlock(1000), nullptr);

  for (const auto &item : handles) {
    buffer.releaseBlock(item);
  }

  std::unordered_set<int> blockIdxSet;
  for (uint i = 0; i < blockNum * 2; i++) {
    BlockBufferHandle item = buffer.getBlock(1001 + i);
    EXPECT_NE(item, nullptr);
    blockIdxSet.emplace(item->getIndex());
    EXPECT_LT(blockIdxSet.size(), blockNum);
    EXPECT_EQ(blockIdxSet.find(index), blockIdxSet.end());
    buffer.releaseBlock(item);
  }
}

TEST_F(BlockBufferSplitTest, TwoBlocks) {
  const int index = 1;

  for (block_no_t i : {1000, 1001}) {
    BlockBufferHandle item = buffer.getBlock(i, index);
    EXPECT_NE(item, nullptr);
    EXPECT_EQ(item->getIndex(), index);
    buffer.releaseBlock(item);
  }

  {
    std::vector<ExportedBlockBufferItem> itemSet;
    buffer.splitBufferItemsByIndex(index, itemSet);
    EXPECT_EQ(itemSet.size(), 2);
  }

  buffer.forEachBlock([&](block_no_t bno, BlockBufferHandle item) {
    EXPECT_NE(item->getIndex(), index);
  });
}

TEST(BlockBufferMigrationTest, SplitAndInstall) {
  constexpr uint32_t blockNum = 4;
  constexpr int blockSize = 32;
  char memPtrSrc[blockNum * blockSize]{};
  char memPtrDst[blockNum * blockSize]{};
  BlockBuffer bufferSrc(blockNum, blockSize, memPtrSrc);
  BlockBuffer bufferDst(blockNum, blockSize, memPtrDst);

  const int index = 1;

  // put 1000 to the src buffer
  {
    BlockBufferHandle item = bufferSrc.getBlock(1000, index);
    EXPECT_NE(item, nullptr);
    bufferSrc.releaseBlock(item);
  }

  // move 1000 out to itemSet
  std::vector<ExportedBlockBufferItem> itemSet;
  bufferSrc.splitBufferItemsByIndex(index, itemSet);
  EXPECT_EQ(itemSet.size(), 1);

  // fill the dst buffer
  {
    std::vector<BlockBufferHandle> handles;
    std::unordered_set<int> dstBlockIdxSet;
    for (uint i = 0; i < blockNum; i++) {
      BlockBufferHandle item = bufferDst.getBlock(2000 + i, i);
      EXPECT_NE(item, nullptr);
      dstBlockIdxSet.emplace(item->getIndex());
      bufferDst.releaseBlock(item);
    }

    EXPECT_EQ(dstBlockIdxSet.size(), blockNum);
    EXPECT_NE(dstBlockIdxSet.find(index), dstBlockIdxSet.end());
  }

  /* FIXME: this install test may be problematic because it requires a map from
   * aid to AppProc*; we disable this piece for now.*/
  /*****************************************************************************
  // now we install
  {
    bufferDst.installBufferItemsOfIndex(index, itemSet,
                                        std::unordered_map<pid_t, AppProc *>{});
    BlockBufferHandle item = bufferDst.getBlock(1000, index);
    EXPECT_NE(item, nullptr);
    EXPECT_EQ(item->getIndex(), index);
    bufferDst.releaseBlock(item);
  }
  *****************************************************************************/
}
}  // namespace

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
