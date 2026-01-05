
#include "BlockBuffer.h"
#include "gtest/gtest.h"

namespace {
using namespace blockbuffer;

class BlockBufferTest : public ::testing::Test {
 protected:
  static constexpr uint32_t blockNum = 4;
  static constexpr int blockSize = 32;
  char memPtr[blockNum * blockSize]{};
  BlockBuffer buffer{blockNum, blockSize, memPtr};
};

TEST_F(BlockBufferTest, SingleBlock) {
  auto handle = buffer.getBlock(1000);
  EXPECT_NE(handle, nullptr);
  EXPECT_EQ(handle.get_key(), 1000);
  EXPECT_TRUE(handle->getBufPtr() >= memPtr);
  EXPECT_TRUE(handle->getBufPtr() < memPtr + blockNum * blockSize);
  EXPECT_FALSE(handle->isInMem());
  EXPECT_FALSE(handle->isDirty());
  buffer.releaseBlock(handle);
}

TEST_F(BlockBufferTest, GetTwice) {
  for (block_no_t no : {1000, 1001, 1002, 1003}) {
    auto handle = buffer.getBlock(no);
    EXPECT_NE(handle, nullptr);
    EXPECT_EQ(handle, buffer.getBlock(no));
    buffer.releaseBlock(handle);
    buffer.releaseBlock(handle);
  }
}

TEST_F(BlockBufferTest, Full) {
  std::vector<BlockBufferHandle> handles;

  for (block_no_t no : {1000, 1001, 1002, 1003}) {
    auto handle = buffer.getBlock(no);
    EXPECT_NE(handle, nullptr);
    handles.push_back(handle);
  }
  // cache full, not supposed to get one buffer
  EXPECT_EQ(buffer.getBlock(1004), nullptr);
  EXPECT_EQ(buffer.getBlock(1005), nullptr);

  // release two blocks
  buffer.releaseBlock(handles.front());
  handles.erase(handles.begin());
  buffer.releaseBlock(handles.front());
  handles.erase(handles.begin());

  // should be able to get two blocks
  for (block_no_t no : {1004, 1005}) {
    auto handle = buffer.getBlock(no);
    EXPECT_NE(handle, nullptr);
    handles.push_back(handle);
  }

  // clean up
  for (auto handle : handles) {
    buffer.releaseBlock(handle);
  }
}

/* This test is disabled because we no longer support getBlockWithoutAccess */
/*******************************************************************************
TEST_F(BlockBufferTest, Replacement) {
  for (block_no_t no : {1000, 1001, 1002, 1003}) {
    auto handle = buffer.getBlock(no);
    EXPECT_NE(handle, nullptr);
    buffer.releaseBlock(handle);
  }

  // should still in cache
  for (block_no_t no : {1000, 1001, 1002, 1003}) {
    EXPECT_NE(buffer.getBlockWithoutAccess(no), nullptr);
  }

  // Use 1004-1007 to replace all the four items
  for (block_no_t no : {1004, 1005, 1006, 1007}) {
    auto handle = buffer.getBlock(no);
    EXPECT_NE(handle, nullptr);
    buffer.releaseBlock(handle);
  }

  // 1000-1003 should be replaced
  for (block_no_t no : {1000, 1001, 1002, 1003}) {
    EXPECT_EQ(buffer.getBlockWithoutAccess(no), nullptr);
  }

  // First use 1004, make it the last one to be replaced
  {
    auto handle = buffer.getBlock(1004);
    EXPECT_NE(handle, nullptr);
    buffer.releaseBlock(handle);
  }

  // Use 1008 to replace 1005
  {
    auto handle = buffer.getBlock(1008);
    EXPECT_NE(handle, nullptr);
    buffer.releaseBlock(handle);
  }

  // 1005 should be replaced
  EXPECT_EQ(buffer.getBlockWithoutAccess(1005), nullptr);
  // we should have 1004, 1006, 1007, 1008
  for (block_no_t no : {1004, 1006, 1007, 1008}) {
    EXPECT_NE(buffer.getBlockWithoutAccess(no), nullptr);
  }
}
*******************************************************************************/

TEST_F(BlockBufferTest, Dirty) {
  auto handle = buffer.getBlock(1000);
  EXPECT_NE(handle, nullptr);
  buffer.setBlockDirty(handle, 0);
  EXPECT_TRUE(handle->isDirty());

  EXPECT_EQ(buffer.flusher.getDirtyItemNum(), 1);

  buffer.unsetBlockDirty(handle);
  EXPECT_FALSE(handle->isDirty());

  EXPECT_EQ(buffer.flusher.getDirtyItemNum(), 0);

  buffer.releaseBlock(handle);
}

TEST(BlockBufferFlusherTest, FlusherTest) {
  constexpr uint32_t blockNum = 800;
  constexpr int blockSize = 32;
  char memPtr[blockNum * blockSize]{};
  BlockBuffer buffer(blockNum, blockSize, memPtr);
  Flusher* flusher = &buffer.flusher;

  // This is actually the default setting. but explicitly set them here in
  // case that default setting is changed.
  flusher->setDirtyRatio(0.2);
  flusher->setDirtyFlushOneTimeSubmitNum(100);

  bool canFlush = true;

  block_no_t flushBlockNumThreshold = blockNum * 0.2;
  block_no_t curBlockNo = 1000;
  block_no_t curBlockNum = 0;
  for (; curBlockNum < flushBlockNumThreshold; curBlockNum++) {
    auto item = buffer.getBlock(curBlockNo++);
    buffer.setBlockDirty(item, 0);
    buffer.releaseBlock(item);
  }

  EXPECT_EQ(flusher->getDirtyItemNum(), curBlockNum);
  EXPECT_FALSE(flusher->checkIfNeedBgFlush());

  curBlockNum++;
  auto item = buffer.getBlock(curBlockNo++);
  buffer.setBlockDirty(item, 0);
  buffer.releaseBlock(item);

  // now, the buffer can be flushed.
  EXPECT_TRUE(flusher->checkIfNeedBgFlush());

  std::list<BlockBufferHandle> flushBlocks;
  flusher->doFlush(canFlush, flushBlocks);
  EXPECT_EQ(flushBlocks.size(), 100);
  EXPECT_TRUE(canFlush);
  // we need to do this accounting
  flusher->addFgFlushInflightNum(1);

  // Cannot submit another flush request if the last one has not been done.
  std::list<BlockBufferHandle> flushBlocksEmpty;
  flusher->setFgFlushLimit(1);
  flusher->doFlush(canFlush, flushBlocksEmpty);
  EXPECT_FALSE(canFlush);
  EXPECT_TRUE(flushBlocksEmpty.empty());

  for (auto ele : flushBlocks) buffer.unsetBlockDirty(ele);
  int rc = flusher->doFlushDone();
  EXPECT_EQ(rc, 0);
  flusher->addFgFlushInflightNum(-1);

  EXPECT_EQ(flusher->getDirtyItemNum(), curBlockNum - 100);
  EXPECT_FALSE(flusher->checkIfNeedBgFlush());

  flushBlocks.clear();
  flusher->doFlush(canFlush, flushBlocks);
  EXPECT_TRUE(canFlush);
  EXPECT_EQ(flushBlocks.size(), std::min(curBlockNum - 100, (uint32_t)100));
  flusher->addFgFlushInflightNum(1);
  for (auto ele : flushBlocks) buffer.unsetBlockDirty(ele);
  flusher->doFlushDone();
  EXPECT_EQ(flusher->getDirtyItemNum(), 0);
  flusher->addFgFlushInflightNum(-1);
}
}  // namespace

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
