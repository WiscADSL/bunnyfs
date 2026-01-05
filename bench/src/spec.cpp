#include "spec.h"

#include <spdlog/spdlog.h>

#include <fstream>
#include <nlohmann/json.hpp>

#include "args.h"

namespace spec {

NLOHMANN_DEFINE_TYPE_NON_INTRUSIVE(File, path, size)
NLOHMANN_DEFINE_TYPE_NON_INTRUSIVE(Database, path, num_keys, value_size)

NLOHMANN_DEFINE_TYPE_NON_INTRUSIVE(Prep, files, databases)

NLOHMANN_JSON_SERIALIZE_ENUM(OffsetType, {{OffsetType::UNIF, "unif"},
                                          {OffsetType::SHUFFLE, "shuffle"},
                                          {OffsetType::ZIPF, "zipf"},
                                          {OffsetType::SEQ, "seq"},
                                          {OffsetType::MIXGRAPH, "mixgraph"}})

NLOHMANN_DEFINE_TYPE_NON_INTRUSIVE(Offset, type, min, max, align, theta)
std::string Offset::dump() const { return nlohmann::json(*this).dump(); }

NLOHMANN_DEFINE_TYPE_NON_INTRUSIVE_WITH_DEFAULT(Workload, name, ops,
                                                duration_sec, count, qdepth,
                                                offset, read_ratio,
                                                dirty_threshold)

std::string Workload::dump() const { return nlohmann::json(*this).dump(); }

NLOHMANN_JSON_SERIALIZE_ENUM(ThreadType,
                             {{ThreadType::RW, "rw"}, {ThreadType::DB, "db"}})

NLOHMANN_DEFINE_TYPE_NON_INTRUSIVE_WITH_DEFAULT(Thread, type, name, core,
                                                worker_id, file_paths,
                                                pin_file_map, db_path,
                                                workloads)

NLOHMANN_DEFINE_TYPE_NON_INTRUSIVE_WITH_DEFAULT(App, aid, name, desc, threads)

NLOHMANN_DEFINE_TYPE_NON_INTRUSIVE_WITH_DEFAULT(Expr, name, desc, prep,
                                                num_workers, apps)
void Expr::dump(const std::filesystem::path& path) const {
  SPDLOG_INFO("Dumping spec to {}", path.string());
  std::ofstream config_file(path);
  if (!config_file) {
    SPDLOG_ERROR("Failed to write config file: {}", path.string());
    exit(1);
  }
  config_file << nlohmann::json(*this).dump(2);
}

Expr Expr::load(const std::filesystem::path& path) {
  SPDLOG_INFO("Loading expr from file: {}", path.string());
  std::ifstream config_file(path);
  if (!config_file) {
    SPDLOG_ERROR("Failed to read config file: {}", path.string());
    exit(1);
  }
  auto json = nlohmann::json::parse(config_file,
                                    /*cb=*/nullptr, /*allow_exceptions=*/true,
                                    /*ignore_comments=*/true);
  return json.get<Expr>();
}

void Expr::init(const Args& args_) {
  for (auto& app : apps) {
    for (auto& thread : app.threads) {
      thread.log_path = args_.output_path / (thread.name + ".log");
    }
  }
}

Expr Expr::load(const Args& args) {
  Expr expr = Expr::load(args.spec_path);
  expr.init(args);
  return expr;
}

}  // namespace spec
