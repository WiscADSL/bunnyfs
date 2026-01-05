#pragma once

#include <spdlog/spdlog.h>

#include <chrono>
#include <cstdint>
#include <functional>
#include <utility>
#include <vector>

class Timer {
  std::chrono::high_resolution_clock::time_point start =
      std::chrono::high_resolution_clock::now();

 public:
  void reset() { start = std::chrono::high_resolution_clock::now(); }
  [[nodiscard]] std::chrono::nanoseconds elapsed() const {
    return std::chrono::high_resolution_clock::now() - start;
  }
  [[nodiscard]] std::chrono::nanoseconds elapsed(
      std::chrono::time_point<std::chrono::high_resolution_clock> ts) const {
    return ts - start;
  }
  [[nodiscard]] static std::chrono::time_point<
      std::chrono::high_resolution_clock>
  now() {
    return std::chrono::high_resolution_clock::now();
  }
};

class Stat {
 public:
  struct Info {
    uint64_t ops;
    std::chrono::nanoseconds elapsed;
    std::chrono::nanoseconds latency_sum;

    [[nodiscard]] double get_elapsed_sec() const {
      return std::chrono::duration<double>(elapsed).count();
    }

    [[nodiscard]] double get_iops() const { return ops / get_elapsed_sec(); }
    [[nodiscard]] double get_mbps(uint64_t count) const {
      return get_iops() * count / 1024 / 1024;
    }
    [[nodiscard]] double get_latency_us_per_op() const {
      return std::chrono::duration<double, std::micro>(latency_sum).count() /
             ops;
    }

    void reset() {
      ops = 0;
      elapsed = std::chrono::nanoseconds(0);
      latency_sum = std::chrono::nanoseconds(0);
    }

    Info& operator+=(const Info& rhs) {
      ops += rhs.ops;
      elapsed += rhs.elapsed;
      latency_sum += rhs.latency_sum;
      return *this;
    }
  };

  using Callback = std::function<void(const Stat&)>;

  struct Args {
    const Callback final_callback = nullptr;
    const Callback epoch_callback = nullptr;
    const std::chrono::nanoseconds report_interval = std::chrono::seconds(1);
  };

  explicit Stat(Args args, uint32_t timer_cnt)
      : args(std::move(args)), timers(timer_cnt) {}

  ~Stat() {
    accum += epoch;
    epoch.reset();
    if (args.final_callback) args.final_callback(*this);
  }

  void op_start(uint32_t timer_idx = 0) {
    assert(timer_idx < timers.size());
    timers[timer_idx].reset();
  }

  void op_stop(uint32_t timer_idx = 0) {
    assert(timer_idx < timers.size());
    auto ts = Timer::now();
    epoch.ops++;
    epoch.latency_sum += timers[timer_idx].elapsed(ts);
    auto epoch_elapsed = epoch_elapsed_timer.elapsed(ts);
    if (epoch_elapsed >= args.report_interval) {
      epoch.elapsed = epoch_elapsed;
      accum += epoch;
      if (args.epoch_callback) args.epoch_callback(*this);
      epoch.reset();
      epoch_elapsed_timer.reset();
    }
  }

  // called before the first op_start
  void reset() { epoch_elapsed_timer.reset(); }

  [[nodiscard]] const Info& get_accum_info() const { return accum; }
  [[nodiscard]] const Info& get_epoch_info() const { return epoch; }

 private:
  const Args args;
  std::vector<Timer> timers;
  Info accum{};
  Info epoch{};
  Timer epoch_elapsed_timer;
};
