#include <fcntl.h>
#include <spdlog/spdlog.h>
#include <unistd.h>

#include "thread.h"
#include "utils/ufs.h"

size_t get_file_size(int fd) {
  struct stat st {};
  if (fs_fstat(fd, &st) != 0) {
    SPDLOG_ERROR("fs_stat failed");
    throw std::runtime_error("fs_stat failed");
  }
  return st.st_size;
}

void read_file(int fd, size_t file_size) {
  size_t chunk_size = 4096;
  void* buf = fs_zalloc(chunk_size);

  fs_lseek(fd, 0, SEEK_SET);
  size_t total_read = 0;
  while (total_read < file_size) {
    size_t to_read = std::min(file_size - total_read, chunk_size);
    if (ssize_t rc = fs_read(fd, buf, to_read); rc != to_read) {
      SPDLOG_ERROR("Reading failed with rc={}, total_read={}", rc, total_read);
    }
    total_read += to_read;
  }
}

int main() {
  spdlog::set_level(spdlog::level::debug);
  std::string path = "f_0";

  UFSContext ctx({1, 11, 21, 31});  // assuming 4 workers

  Thread::assign_worker(0);

  int fd = fs_open(path.c_str(), O_RDWR, 0644);
  SPDLOG_INFO("fd={}", fd);
  size_t file_size = get_file_size(fd);
  SPDLOG_INFO("file_size={}", file_size);
  read_file(fd, file_size);

  Thread::assign_worker(1);
  read_file(fd, file_size);

  Thread::assign_worker(2);
  read_file(fd, file_size);

  Thread::assign_worker(3);
  read_file(fd, file_size);

  Thread::assign_worker(0);
  read_file(fd, file_size);

  fs_close(fd);
}
