#include "args.h"
#include "prepare.h"
#include "spec.h"


int main(int argc, char* argv[]) {
  const Args& args = Args::parse(argc, argv);
  const spec::Expr& expr = spec::Expr::load(args);
  SPDLOG_INFO("Preparing expr \"{}\"", expr.name);
  prepare_main(expr.prep);
}
