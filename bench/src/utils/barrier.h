#pragma once

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/user.h>

#include <stdexcept>
#include <string>

class Barrier {
  struct alignas(PAGE_SIZE) ShmBuf {
    int count;
  };

  size_t num;
  ShmBuf* buf;

 public:
  explicit Barrier(size_t num = 0)
      : num(num),
        buf((ShmBuf*)mmap(nullptr, sizeof(ShmBuf), PROT_READ | PROT_WRITE,
                          MAP_SHARED | MAP_ANONYMOUS, -1, 0)) {
    if (buf == MAP_FAILED) throw std::runtime_error("mmap failed");
    buf->count = 0;
  }
  ~Barrier() { munmap(buf, sizeof(ShmBuf)); }

  void arrive_and_wait() const {
    if (__sync_add_and_fetch(&buf->count, 1) == num) {
      buf->count = 0;
    } else {
      while (buf->count != 0) {
        __builtin_ia32_pause();
      }
    }
  }
};
