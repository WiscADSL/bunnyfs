#pragma once

#include <spdlog/fmt/fmt.h>

#include <filesystem>

struct Args {
  std::filesystem::path spec_path;
  std::filesystem::path output_path;

  static Args parse(int argc, char* argv[]);

  [[nodiscard]] std::string dump() const {
    return fmt::format("spec_path: {}, output_path: {}", spec_path.string(),
                       output_path.string());
  }
};
