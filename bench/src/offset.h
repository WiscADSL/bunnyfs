#pragma once

#include <fcntl.h>

#include <random>
#include <stdexcept>
#include <vector>

#include "spec.h"
#include "utils/mixgraph.h"

struct BaseGenerator {
  size_t index;  // The number of times get() has been called

  BaseGenerator() : index(0) {}
  BaseGenerator(const BaseGenerator&) = delete;
  virtual ~BaseGenerator() = default;
  virtual off_t get() = 0;
  void next() { index++; }
};

struct AlignGenerator : public BaseGenerator {
  const off_t min_;
  const off_t align_;
  /* We force min and max must be aligned.
   * This also ensure [output, output + align - 1] is also safe (i.e. < max)
   */
  AlignGenerator(off_t min, off_t max, off_t align) : min_(min), align_(align) {
    if (min % align != 0) throw std::runtime_error("min is not aligned!");
    if (max % align != 0) throw std::runtime_error("max is not aligned!");
  }
  [[nodiscard]] off_t map(off_t x) const { return min_ + align_ * x; }
};

struct SeqGenerator : public AlignGenerator {
  off_t n_;
  SeqGenerator(off_t min, off_t max, off_t align)
      : AlignGenerator(min, max, align), n_((max - min) / align) {}
  off_t get() override { return map(index % n_); }
};

struct ShuffleGenerator : public BaseGenerator {
  std::vector<off_t> offsets;
  ShuffleGenerator(off_t min, off_t max, off_t align) {
    auto seq_gen = SeqGenerator(min, max, align);
    offsets.reserve(seq_gen.n_);
    for (size_t i = 0; i < seq_gen.n_; i++) {
      offsets.push_back(seq_gen.get());
      seq_gen.next();
    }
    std::random_device rd;
    std::mt19937 rng(rd());
    std::shuffle(offsets.begin(), offsets.end(), rng);
  }
  off_t get() override { return offsets[index % offsets.size()]; }
};

struct UnifGenerator : public AlignGenerator {
  std::mt19937 rng{std::random_device{}()};
  std::uniform_int_distribution<off_t> dist; /* inclusive */
  UnifGenerator(off_t min, off_t max, off_t align)
      : AlignGenerator(min, max, align), dist(0, (max - min) / align - 1) {}
  off_t get() override { return map(dist(rng)); }
};

static double zeta(uint64_t n, double theta) {
  double sum = 0;
  for (uint64_t i = 1; i <= n; i++) sum += std::pow(1.0 / i, theta);
  return sum;
}

struct ZipfGenerator : public AlignGenerator {
  std::mt19937 rng{std::random_device{}()};
  std::uniform_real_distribution<double> dist{0.0, 1.0};
  const double theta_;
  const uint64_t n_;
  const double denom_;
  const double eta_;
  const double alpha_;
  // NOTE: member order matters: variable must be init in order!

  ZipfGenerator(off_t min, off_t max, double theta, off_t align)
      : AlignGenerator(min, max, align),
        theta_(theta),
        n_((max - min) / align),
        denom_(zeta(n_, theta)),
        eta_((1 - std::pow(2.0 / n_, 1 - theta)) /
             (1 - zeta(2, theta) / denom_)),
        alpha_(1.0 / (1.0 - theta)) {}
  off_t get() override {
    double u = dist(rng);
    double uz = u * denom_;
    if (uz < 1.0) return map(0);
    if (uz < 1.0 + std::pow(0.5, theta_)) return map(1);
    return map(n_ * std::pow(eta_ * u - eta_ + 1, alpha_));
  }
};

struct MixgraphGenerator : public BaseGenerator {
  // Values set based on paper
  // https://www.usenix.org/system/files/fast20-cao_zhichao.pdf and docs at
  // https://github.com/facebook/rocksdb/wiki/RocksDB-Trace,-Replay,-Analyzer,-and-Workload-Generation#synthetic-workload-generation-based-on-models
  static constexpr double keyrange_dist_a = 14.18;
  static constexpr double keyrange_dist_b = -2.917;
  static constexpr double keyrange_dist_c = 0.0164;
  static constexpr double keyrange_dist_d = -0.08082;
  static constexpr double key_dist_a = 0.002312;
  static constexpr double key_dist_b = 0.3467;
  static constexpr int64_t keyrange_num = 30;

  std::mt19937 rng{std::random_device{}()};
  std::uniform_int_distribution<int64_t> dist{
      0, std::numeric_limits<int64_t>::max()};

  GenerateTwoTermExpKeys gen_exp;

  explicit MixgraphGenerator(off_t min, off_t max, off_t align)
      : gen_exp(max, keyrange_num) {
    if (min != 0) throw std::runtime_error("min must be 0");
    if (align != 1) throw std::runtime_error("align must be 1");

    gen_exp.InitiateExpDistribution(max, keyrange_dist_a, keyrange_dist_b,
                                    keyrange_dist_c, keyrange_dist_d);
  }

  off_t get() override {
    int64_t ini_rand = dist(rng);
    return gen_exp.DistGetKeyID(ini_rand, key_dist_a, key_dist_b);
  }
};

struct Offsets {
  size_t num;
  BaseGenerator* gen;

  struct EndIterator : std::iterator<std::input_iterator_tag, off_t> {
    size_t num;
    explicit EndIterator(size_t num) : num(num) {}
  };

  struct Iterator : std::iterator<std::input_iterator_tag, off_t> {
    BaseGenerator& gen;
    explicit Iterator(BaseGenerator& gen) : gen(gen) {}
    Iterator& operator++() {
      gen.next();
      return *this;
    }
    off_t operator*() const { return gen.get(); }
    bool operator!=(const EndIterator& other) const {
      return gen.index < other.num;
    }
  };
  static_assert(sizeof(Iterator) == 8);  // Iterator needs to be small

  Offsets(size_t num, const spec::Offset& spec)
      : num(num), gen(get_generator(spec)) {}
  Offsets(const Offsets&) = delete;
  ~Offsets() { delete gen; }

  [[nodiscard]] Iterator begin() const { return Iterator(*gen); }
  [[nodiscard]] EndIterator end() const { return EndIterator(num); }
  [[nodiscard]] size_t size() const { return num; }

  static BaseGenerator* get_generator(const spec::Offset& spec) {
    switch (spec.type) {
      /* min <= offset, offset + align - 1 < max */
      case spec::OffsetType::SEQ:
        return new SeqGenerator(spec.min, spec.max, spec.align);
      case spec::OffsetType::SHUFFLE:
        return new ShuffleGenerator(spec.min, spec.max, spec.align);
      case spec::OffsetType::UNIF:
        return new UnifGenerator(spec.min, spec.max, spec.align);
      case spec::OffsetType::ZIPF:
        return new ZipfGenerator(spec.min, spec.max, spec.theta, spec.align);
      case spec::OffsetType::MIXGRAPH:
        return new MixgraphGenerator(spec.min, spec.max, spec.align);
      default:
        throw std::runtime_error("Unimplemented offset type");
    }
  }
};
