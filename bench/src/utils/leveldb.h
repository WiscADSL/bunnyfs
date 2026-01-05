#pragma once

#include <leveldb/db.h>
#include <spdlog/spdlog.h>

#include <filesystem>

static leveldb::DB* open_db(const std::filesystem::path& db_path) {
  // The database exists and open it.
  leveldb::Options options;
  options.create_if_missing = false;
  options.error_if_exists = false;
  options.write_buffer_size = 0;

  leveldb::DB* db = nullptr;
  leveldb::Status status = leveldb::DB::Open(options, db_path, &db);

  if (!status.ok()) {
    SPDLOG_ERROR("Failed to open database {}: {}", db_path.string(),
                 status.ToString());
    throw std::runtime_error("Failed to open database");
  }

  SPDLOG_INFO("Opened existing database \"{}\"", db_path.string());
  return db;
}

static leveldb::DB* open_or_create_db(const std::filesystem::path& db_path) {
  // Assuming the database does not exist and create a new one.
  leveldb::Options options;
  options.create_if_missing = true;
  options.error_if_exists = true;

  leveldb::DB* db = nullptr;
  leveldb::Status status = leveldb::DB::Open(options, db_path, &db);

  if (status.ok()) {
    SPDLOG_INFO("Created empty database \"{}\"", db_path.string());
    return db;
  }

  // The database already exists, so try to open it.
  if (status.ToString().find("error_if_exists") != std::string::npos) {
    return open_db(db_path);
  }

  // Other error while creating the database.
  SPDLOG_ERROR("Failed to open database \"{}\": {}", db_path.string(),
               status.ToString());
  throw std::runtime_error("Failed to open database");
}

static std::string zero_pad(const std::string& str, size_t len) {
  return std::string(len - std::min(len, str.length()), '0') + str;
}

static std::string get_key_from_idx(uint64_t idx, size_t len = 20,
                                    bool do_hash = false) {
  auto idx_str = std::to_string(idx);
  if (!do_hash) return zero_pad(idx_str, len);
  auto hash = std::hash<std::string>{}(idx_str);
  auto hash_str = std::to_string(hash);
  return zero_pad(hash_str, len);
}

static std::string get_value(size_t len) { return std::string(len, 'a'); }

static bool check_prepared(leveldb::DB* db, const spec::Database& db_spec,
                           bool smoke = false) {

  auto expected_value = get_value(db_spec.value_size);

  leveldb::Iterator* it = db->NewIterator(leveldb::ReadOptions());
  it->SeekToFirst();

  std::string value;
  leveldb::Status status;

  for (uint64_t i = 0; i < db_spec.num_keys; i++) {
    if (!it->Valid()) {
      status = it->status();

      // database error, critical
      if (!status.ok()) {
        SPDLOG_ERROR("Database error while reading the {}-th key: {}", i,
                     status.ToString());
        throw std::runtime_error("Database error");
      } else {
        return false;
      }
    }

    value = it->value().ToString();

    // value is wrong
    if (value != expected_value) {
      SPDLOG_ERROR(
          "Value mismatch in db {}: expected \"{}\", got \"{}\"",
           db_spec.path, expected_value, value);
      throw std::runtime_error("Value mismatch");
    }

    it->Next();

    if (smoke) break;  // only check the first key in smoke mode
  }

  // All keys found and values are correct
  return true;
}

static std::vector<uint64_t> get_seq_indices(uint64_t num_keys) {
  std::vector<uint64_t> indices(num_keys);
  std::iota(indices.begin(), indices.end(), 0);
  return indices;
}

static std::vector<uint64_t> get_random_indices(uint64_t num_keys) {
  auto indices = get_seq_indices(num_keys);
  std::random_device rd;
  std::mt19937 g(rd());
  std::shuffle(indices.begin(), indices.end(), g);
  return indices;
}

static void insert_keys(leveldb::DB* db, const spec::Database& db_spec) {
  leveldb::Status status;
  leveldb::WriteOptions write_options;
  auto value = get_value(db_spec.value_size);

  std::vector<uint64_t> indices = get_seq_indices(db_spec.num_keys);
  for (uint64_t i : indices) {
    std::string key = get_key_from_idx(i);
    status = db->Put(write_options, key, value);
    if (!status.ok()) {
      SPDLOG_ERROR("Failed to put key {} in db {}: {}", key, db_spec.path,
                   status.ToString());
      throw std::runtime_error("Failed to prepare database");
    }
  }
}
