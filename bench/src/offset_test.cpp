#include "offset.h"

#include <gtest/gtest.h>
#include <spdlog/spdlog.h>

static void print(const spec::Offset& spec, const Offsets& offsets) {
  // Build comma-separated list of offsets
  std::string offsets_str;
  bool first = true;
  for (auto off : offsets) {
    if (!first) offsets_str += ", ";
    offsets_str += std::to_string(off);
    first = false;
  }
  fmt::print("{:60}: {}\n", spec.dump(), offsets_str);
}

TEST(OffsetTest, Seq) {
  spec::Offset spec{
      .type = spec::OffsetType::SEQ,
      .min = 1,
      .max = 9,
      .align = 3,
  };
  print(spec, Offsets(10, spec));

  Offsets offsets(10, spec);
  std::vector<off_t> expected{3, 6, 3, 6, 3, 6, 3, 6, 3, 6};
  std::vector<off_t> actual;
  actual.reserve(offsets.size());
  for (auto off : offsets) actual.emplace_back(off);
  EXPECT_EQ(actual, expected);
}

TEST(OffsetTest, Shuffle) {
  spec::Offset spec{
      .type = spec::OffsetType::SHUFFLE,
      .min = 2,
      .max = 8,
      .align = 2,
  };
  print(spec, Offsets(12, spec));

  Offsets offsets(12, spec);
  EXPECT_EQ(offsets.size(), 12);
  std::vector<off_t> actual;
  actual.reserve(offsets.size());
  for (auto off : offsets) actual.emplace_back(off);
  EXPECT_EQ(std::count(actual.begin(), actual.end(), 2), 4);
  EXPECT_EQ(std::count(actual.begin(), actual.end(), 4), 4);
  EXPECT_EQ(std::count(actual.begin(), actual.end(), 6), 4);
}

TEST(OffsetTest, Unif) {
  spec::Offset spec{
      .type = spec::OffsetType::UNIF,
      .min = 1,
      .max = 5,
      .align = 2,
  };
  print(spec, Offsets(10, spec));

  Offsets offsets(10, spec);
  EXPECT_EQ(offsets.size(), 10);
  for (auto off : offsets) {
    EXPECT_TRUE(off == 2 || off == 4);
  }
}

TEST(OffsetTest, Zipf) {
  spec::Offset spec{
      .type = spec::OffsetType::ZIPF,
      .min = 1,
      .max = 9,
      .align = 2,
      .theta = 1.2,
  };
  print(spec, Offsets(10, spec));

  Offsets offsets(10, spec);
  EXPECT_EQ(offsets.size(), 10);
  for (auto off : offsets) {
    EXPECT_TRUE(off == 2 || off == 4 || off == 6 || off == 8);
  }
}

void print_histogram(const spec::Offset& spec, const Offsets& offsets,
                     uint64_t num_buckets = 100) {
  std::vector<int> buckets(num_buckets);
  for (auto off : offsets) {
    buckets[off / (spec.max / num_buckets)]++;
  }
  uint64_t max = *std::max_element(buckets.begin(), buckets.end());
  for (int i = 0; i < num_buckets; i++) {
    fmt::print("{:3}: {}\n", i, std::string(buckets[i] * 100 / max, '*'));
  }
}

TEST(OffsetTest, Mixgraph) {
  spec::Offset spec{
      .type = spec::OffsetType::MIXGRAPH,
      .min = 0,
      .max = 5'000,
      .align = 1,
  };
  print_histogram(spec, Offsets(10'000, spec));
  /** Sample output:
    0:
    1:
    2:
    3:
    4:
    5:
    6:
    7:
    8:
    9:
    10:
    11:
    12:
    13:
    14:
    15:
    16:
    17:
    18:
    19:
    20:
    21:
    22:
    23:
    24: *
    25: *
    26:
    27:
    28:
    29:
    30: *
    31: *
    32: *
    33:
    34:
    35:
    36:
    37:
    38:
    39:
    40:
    41:
    42:
    43:
    44:
    45:
    46:
    47: *
    48: *
    49: *
    50:
    51:
    52: *
    53:
    54: *
    55: *
    56:
    57:
    58:
    59:
    60:
    61:
    62: *
    63:
    64:
    65:
    66:
    67:
    68:
    69:
    70:
    71:
    72:
    73:
    74:
    75:
    76: ****
    77: ******
    78: ******
    79: *****
    80:
    81:
    82:
    83:
    84:
    85:
    86:
    87:
    88:
    89:
    90: *
    91: *
    92: *
    93:
    94:
    95:
    96: *************************************************************
    97: *****************************************************************************************
    98: ****************************************************************************************************
    99: ****************************************************************
   */
}
