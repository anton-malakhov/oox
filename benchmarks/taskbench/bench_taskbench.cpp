#include "runner_folly.h"
#include "runner_oox.h"
#include "runner_openmp.h"
#include "runner_serial.h"
#include "runner_taskflow.h"
#include "runner_tbb_flow.h"
#include "taskbench_cli.h"

#include <benchmark/benchmark.h>

#include <exception>
#include <iostream>
#include <string>

#if HAVE_TBB
#ifndef TBB_USE_ASSERT
#define TBB_USE_ASSERT 0
#endif
#include <optional>
#include <tbb/global_control.h>
#endif

namespace tb = oox_bench::taskbench;

namespace {

const char* backend_name() {
#if HAVE_TBB
  return "TBB";
#elif HAVE_OMP
  return "OMP";
#elif HAVE_TF
  return "TF";
#elif HAVE_FOLLY
  return "FOLLY";
#elif HAVE_EIGEN
  return "EIGEN";
#elif OOX_SERIAL_DEBUG
  return "SERIAL";
#else
  return "STD";
#endif
}

tb::RunResult run_once(const tb::Cli& cli) {
  switch (cli.runner) {
    case tb::Runner::OOX: return tb::run_oox(cli.config);
    case tb::Runner::Serial: return tb::run_serial(cli.config);
    case tb::Runner::TbbFlow: return tb::run_tbb_flow(cli.config);
    case tb::Runner::Taskflow: return tb::run_taskflow(cli.config);
    case tb::Runner::OpenMP: return tb::run_openmp(cli.config);
    case tb::Runner::Folly: return tb::run_folly(cli.config);
  }
  throw std::runtime_error("unknown runner");
}

void run_benchmark(benchmark::State& state, const tb::Cli& cli) {
  tb::RunResult last;
  bool ok = true;
  for (auto _ : state) {
    last = run_once(cli);
    ok = ok && last.ok;
    benchmark::DoNotOptimize(last.wall_s);
  }

  state.counters["threads"] = last.threads;
  state.counters["width"] = last.width;
  state.counters["tasks"] = static_cast<double>(last.tasks);
  state.counters["edges"] = static_cast<double>(last.edges);
  state.counters["ok"] = ok ? 1.0 : 0.0;
  if (!ok) {
    state.SkipWithError("validation failed");
  }
}

} // namespace

int main(int argc, char** argv) {
  try {
    benchmark::Initialize(&argc, argv);
    const tb::Cli cli = tb::parse_cli(argc, argv);
    if (cli.help) {
      tb::print_help(std::cout, argv[0]);
      return 0;
    }

#if HAVE_TBB
    std::optional<tbb::global_control> tbb_threads;
    if (cli.config.threads > 0) {
      tbb_threads.emplace(tbb::global_control::max_allowed_parallelism,
                          static_cast<std::size_t>(cli.config.threads));
    }
#endif

    const std::string name = std::string("TaskBench/") + backend_name() + "/" + tb::to_string(cli.runner);
    benchmark::RegisterBenchmark(name.c_str(), [cli](benchmark::State& state) { run_benchmark(state, cli); })
        ->UseRealTime()
        ->Unit(benchmark::kSecond);
    benchmark::RunSpecifiedBenchmarks();
    benchmark::Shutdown();
    return 0;
  } catch (const std::exception& ex) {
    std::cerr << "bench_taskbench: " << ex.what() << '\n';
    return 1;
  }
}
