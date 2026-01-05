#pragma once

#include <fcntl.h>
#include <spdlog/spdlog.h>
#include <spdlog/fmt/fmt.h>
#include <sys/wait.h>
#include <unistd.h>

#include <random>

#include "config.h"
#include "fsapi.h"
#include "spec.h"
#include "utils/leveldb.h"
#include "utils/logging.h"
#include "utils/ufs.h"

static std::tuple<bool, uint64_t> get_file_size(const std::string& path) {
  struct stat st {};
  if (fs_stat(path.c_str(), &st) != 0) {
    return {false, 0};
  }
  return {true, st.st_size};
}

/**
 * @brief Prepare the file system for the benchmark.
 * @param file_spec The file specification (path + size).
 * @return The number of bytes written to the file system.
 */
static uint64_t prepare_file(const spec::File& file_spec) {
  uint64_t size_written = 0;
  auto [exist, actual_size] = get_file_size(file_spec.path);
  if (actual_size >= file_spec.size) {
    SPDLOG_INFO("{} already exists with size {:.3f} MB >= {:.3f} MB",
                file_spec.path, (double)actual_size / 1024.0 / 1024.0,
                (double)file_spec.size / 1024.0 / 1024.0);
    return size_written;
  }

  int open_flags = exist ? O_RDWR : O_RDWR | O_CREAT;
  int fd = fs_open(file_spec.path.c_str(), open_flags, 0644);
  if (fd < 0) {
    throw std::runtime_error("open failed");
  }

  SPDLOG_INFO("File \"{}\" size {:.3f} MB < {:.3f} MB, preparing...",
              file_spec.path, (double)actual_size / 1024 / 1024,
              (double)file_spec.size / 1024 / 1024);
  uint64_t diff = file_spec.size - actual_size;
  uint64_t chunk_size = 2 * 1024 * 1024;

  void* buf = fs_zalloc(chunk_size);
  if (buf == nullptr) {
    throw std::runtime_error("fs_zalloc failed");
  }
  while (diff > 0) {
    size_t to_write = std::min(diff, chunk_size);
    if (ssize_t rc = fs_write(fd, buf, to_write); rc != to_write) {
      SPDLOG_ERROR("Writing to file \"{}\" failed with rc={}, size_written={}",
                   file_spec.path, rc, size_written);
      throw std::runtime_error("write failed");
    }
    fs_fsync(fd);  // It is important to call fsync() after each write so that
                   // the blocks are not pinned in the cache.
    diff -= to_write;
    size_written += to_write;
  }
  fs_free(buf);

  close(fd);

  fs_syncall();
  return size_written;
}

/**
 * @return number of keys inserted
 */
static uint64_t prepare_db(leveldb::DB* db, const spec::Database& db_spec,
                           bool check_after_insert = false,
                           bool print_stats = false) {
  leveldb::Status status;

  // check if the database is already prepared with the correct value size
  if (bool is_prepared = check_prepared(db, db_spec); is_prepared) {
    SPDLOG_INFO(
        "Verified database \"{}\" prepared with {} keys and value size {}",
        db_spec.path, db_spec.num_keys, db_spec.value_size);
    return 0;
  } else {
    SPDLOG_INFO("Database \"{}\" is not prepared, preparing...", db_spec.path);
  }

  insert_keys(db, db_spec);
  fs_syncall();

  // check again that the database is prepared, this is almost always true,
  // so we do not do this by default
  if (check_after_insert) {
    if (bool is_prepared = check_prepared(db, db_spec); !is_prepared) {
      throw std::runtime_error("Database corrupted after insert");
    } else {
      SPDLOG_INFO("Prepared and verified \"{}\" with {} keys and value size {}",
                  db_spec.path, db_spec.num_keys, db_spec.value_size);
    }
  } else {
    SPDLOG_INFO("Prepared \"{}\" with {} keys with value size {}", db_spec.path,
                db_spec.num_keys, db_spec.value_size);
  }

  // compact the entire database
  db->CompactRange(nullptr, nullptr);
  fs_syncall();

  // print stats
  if (print_stats) {
    std::string stat;
    db->GetProperty("leveldb.stats", &stat);
    fmt::print("{}", stat);
  }

  return db_spec.num_keys;
}

static void prepare_dbs(const std::vector<spec::Database>& specs) {
  std::vector<leveldb::DB*> dbs;
  for (const auto& db_spec : specs) {
    auto db = open_or_create_db(db_spec.path);
    prepare_db(db, db_spec);
    dbs.push_back(db);
    std::this_thread::sleep_for(std::chrono::milliseconds{500});
  }
  // wait for background compaction to finish
  std::this_thread::sleep_for(std::chrono::milliseconds{500});
  for (auto db : dbs) {
    delete db;
  }
}

static void prepare_main(const spec::Prep& prep) {
  if (prep.files.empty() && prep.databases.empty()) {
    SPDLOG_INFO("Nothing to prepare");
    return;
  }

  SPDLOG_INFO("Start preparing");
  {
    UFSContext ctx;
    for (const auto& file_spec : prep.files) prepare_file(file_spec);
    prepare_dbs(prep.databases);
  }
  SPDLOG_INFO("Finished preparing");
}
