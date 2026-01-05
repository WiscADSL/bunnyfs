#include <spdlog/spdlog.h>

#include <vector>

#include "args.h"
#include "prepare.h"
#include "spec.h"
#include "thread.h"
#include "utils/barrier.h"
#include "utils/fork.h"
#include "utils/ufs.h"

void run_app(const spec::Expr& expr, int index, Barrier& barrier) {
  const spec::App& app = expr.apps[index];
  SPDLOG_INFO("App \"{}\" started: {}", app.name, app.desc);

  {
    UFSContext ctx(expr.num_workers, app.aid);

    std::vector<std::thread> threads;
    threads.reserve(app.threads.size());
    for (const auto& thread : app.threads) {
      threads.emplace_back(
          [&, thread]() { Thread::main(thread, barrier); });
    }

    for (auto& thread : threads) {
      thread.join();
    }
  }

  SPDLOG_INFO("App \"{}\" finished", app.name);
}

void run_expr(const spec::Expr& expr) {
  SPDLOG_INFO("Running expr \"{}\": {}", expr.name, expr.desc);

  size_t num_threads = 0;
  for (const auto& app : expr.apps) num_threads += app.threads.size();

  Barrier barrier(num_threads);
  fork_and_wait(expr.apps.size(), [&](int i) { run_app(expr, i, barrier); });

  SPDLOG_INFO("Expr \"{}\" finished", expr.name);
}

int main(int argc, char* argv[]) {
  const Args& args = Args::parse(argc, argv);

  auto logger = create_logger("root", args.output_path / "root.log");
  spdlog::set_default_logger(logger);

  const spec::Expr& expr = spec::Expr::load(args);
  expr.dump(args.output_path / "spec.json");
  run_expr(expr);
  SPDLOG_INFO("Result is at {}", args.output_path.string());
}
