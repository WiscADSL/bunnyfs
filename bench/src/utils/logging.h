#pragma once

#include <spdlog/sinks/basic_file_sink.h>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/spdlog.h>

#include <filesystem>

static std::shared_ptr<spdlog::logger> create_logger(
    const std::string& name, const std::filesystem::path& path) {
  auto logger = std::make_shared<spdlog::logger>(spdlog::logger(
      name, {
                std::make_shared<spdlog::sinks::stdout_color_sink_st>(
                    spdlog::color_mode::always),
                std::make_shared<spdlog::sinks::basic_file_sink_st>(path),
            }));
  if constexpr (config::DEBUG) logger->set_level(spdlog::level::debug);
  return logger;
}

inline thread_local std::shared_ptr<spdlog::logger> thread_local_logger =
    spdlog::default_logger();

static void init_thread_local_logger(const std::string& name,
                                     const std::filesystem::path& path) {
  thread_local_logger = create_logger(name, path);
}

#define THREAD_DEBUG(...) SPDLOG_LOGGER_DEBUG(thread_local_logger, __VA_ARGS__)
#define THREAD_INFO(...) SPDLOG_LOGGER_INFO(thread_local_logger, __VA_ARGS__)
#define THREAD_WARN(...) SPDLOG_LOGGER_WARN(thread_local_logger, __VA_ARGS__)
#define THREAD_ERROR(...) SPDLOG_LOGGER_ERROR(thread_local_logger, __VA_ARGS__)
