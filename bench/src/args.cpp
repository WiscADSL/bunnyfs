#include "args.h"

#include <spdlog/spdlog.h>

#include <cxxopts.hpp>
#include <filesystem>

static std::string gen_timestamp() {
  time_t t = time(nullptr);
  char buf[32];
  strftime(buf, sizeof(buf), "%Y%m%d-%H%M%S", localtime(&t));
  return buf;
}

Args Args::parse(int argc, char* argv[]) {
  Args args = {};

  cxxopts::Options options("bench", "Benchmark");
  options.add_options(
      {}, {{"f,file", "Spec file",
            cxxopts::value<std::filesystem::path>(args.spec_path)},
           {"o,output", "Output directory",
            cxxopts::value<std::filesystem::path>(args.output_path)},
           {"h,help", "Print help"}});

  options.parse_positional("file");
  auto result = options.parse(argc, argv);

  if (result.count("help")) {
    fmt::print("{}\n", options.help());
    exit(0);
  }

  if (args.spec_path.empty()) {
    SPDLOG_ERROR("Missing spec file");
    exit(1);
  }

  if (args.output_path.empty()) {
    auto src_dir = std::filesystem::path(__FILE__).parent_path();
    std::string name = args.spec_path.stem().string() + "-" + gen_timestamp();
    args.output_path = src_dir / "results" / name;
  }

  args.spec_path = std::filesystem::absolute(args.spec_path);
  args.output_path = std::filesystem::absolute(args.output_path);

  std::filesystem::create_directories(args.output_path);

  SPDLOG_INFO("Args: {}", args.dump());

  return args;
}
