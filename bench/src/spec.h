#pragma once

#include <filesystem>
#include <string>
#include <unordered_map>
#include <vector>

struct Args;

namespace spec {
struct Thread;
struct App;
struct Expr;

struct File {
  std::string path;
  size_t size;
};

struct Database {
  std::string path;
  size_t num_keys;
  size_t value_size;
};

struct Prep {
  std::vector<File> files;
  std::vector<Database> databases;
};

enum class OffsetType { UNIF, ZIPF, SEQ, SHUFFLE, MIXGRAPH };

struct Offset {
  OffsetType type;
  off_t min;       // inclusive
  off_t max;       // exclusive
  uint32_t align;  // in bytes
  double theta;    // only for zipf

  [[nodiscard]] std::string dump() const;
};

struct Workload {
  std::string name;
  uint64_t ops = std::numeric_limits<uint64_t>::max();
  uint64_t duration_sec = std::numeric_limits<uint64_t>::max();
  uint64_t count = 4096;  // in bytes
  uint32_t qdepth = 1;    // 1 uses sync APIs; >1 uses async APIs
  Offset offset = {};
  double read_ratio = 1.0;
  uint64_t dirty_threshold;  // max size of dirty data in bytes (per-file)

  [[nodiscard]] std::string dump() const;
};

enum class ThreadType { RW, DB };

struct Thread {
  ThreadType type = ThreadType::RW;
  std::string name;
  int core = -1;       // -1 means no affinity
  int worker_id = -1;  // -1 means no reassignment
  std::vector<std::string> file_paths;
  // map file_path to wid (will overwrite thread-based file pinning)
  std::unordered_map<std::string, int> pin_file_map;
  std::string db_path;
  std::vector<Workload> workloads;

  std::filesystem::path log_path;  // derived from thread.name and result_dir
};

struct App {
  std::string name;
  std::string desc;
  int aid = -1;
  std::vector<Thread> threads;
};

struct Expr {
  std::string name;
  std::string desc;
  int num_workers = 1;
  Prep prep;
  std::vector<App> apps;

  static Expr load(const Args& args);
  void dump(const std::filesystem::path& path) const;

 private:
  static Expr load(const std::filesystem::path& path);
  void init(const Args& args_);
};
}  // namespace spec
