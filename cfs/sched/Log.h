/**
 * This file provide some basic functionality of logging, which could be useful
 * to reason allocation results.
 */
#pragma once

#ifdef USE_NANOLOG
#include "NanoLogCpp17.h"

namespace sched::log {
static inline void init() { NanoLog::setLogLevel(NanoLog::LogLevels::DEBUG); }

static inline void destroy() {
  NanoLog::sync();
  NanoLog::printConfig();
  std::cout << NanoLog::getStats();
}
}  // namespace sched::log

#define SCHED_LOG_DEBUG(msg, ...) NANO_LOG(DEBUG, msg, ##__VA_ARGS__)
#define SCHED_LOG_NOTICE(msg, ...) NANO_LOG(NOTICE, msg, ##__VA_ARGS__)
#define SCHED_LOG_WARNING(msg, ...) NANO_LOG(WARNING, msg, ##__VA_ARGS__)

#else

namespace sched::log {
static inline void init() {}
static inline void destroy() {}
}  // namespace sched::log

#define SCHED_LOG_DEBUG(msg, ...) (void(msg))
#define SCHED_LOG_NOTICE(msg, ...) (void(msg))
#define SCHED_LOG_WARNING(msg, ...) (void(msg))

#endif  // USE_NANOLOG
