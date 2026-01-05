#pragma once

#include <fcntl.h>
#include <spdlog/spdlog.h>
#include <sys/wait.h>
#include <unistd.h>

#include <stdexcept>
#include <vector>

template <typename Fn>
static void fork_and_wait(size_t nproc, const Fn& fn) {
  std::vector<pid_t> pids;
  pids.reserve(nproc);
  for (int i = 0; i < nproc; i++) {
    pid_t pid = fork();
    if (pid == 0) {
      fn(i);
      exit(0);
    } else if (pid > 0) {
      pids.emplace_back(pid);
      SPDLOG_INFO("Forked child {} (pid {})", i, pid);
    } else {
      throw std::runtime_error("fork failed");
    }
  }

  for (int i = 0; i < nproc; i++) {
    int status;
    if (waitpid(pids[i], &status, 0) == -1) {
      throw std::runtime_error("waitpid failed");
    }

    if (WIFEXITED(status) && WEXITSTATUS(status) == 0) {
      SPDLOG_INFO("Child {} (pid {}) exited normally", i, pids[i]);
      continue;
    }

    if (WIFSIGNALED(status)) {
      SPDLOG_WARN("Child {} (pid {}) killed by signal \"{}\".", i, pids[i],
                  strsignal(WTERMSIG(status)));
    } else if (WIFEXITED(status)) {
      SPDLOG_WARN("Child {} (pid {}) exited with code {}.", i, pids[i],
                  WEXITSTATUS(status));
      SPDLOG_WARN(
          "This is fine if this is the prep phase, and child exits with 1");
    } else {
      SPDLOG_WARN("Child {} (pid {}) exited abnormally with status {}. ", i,
                  pids[i], status);
    }

    SPDLOG_WARN("Killing other child processes...");
    for (int j = i; j < nproc; j++) {
      kill(pids[j], SIGTERM);
    }
    exit(1);
  }
}

template <typename Fn>
static void fork_and_wait(const Fn& fn) {
  fork_and_wait(1, [&](int) { fn(); });
}
