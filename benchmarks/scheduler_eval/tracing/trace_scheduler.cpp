// SPDX-License-Identifier: Apache-2.0
#include "recorder.h"
#include "common.h"
#include <atomic>
#include <fstream>
#include <memory>

template <typename F>
void RunTraceLoop(std::size_t tasks, F &&body, std::size_t grain) {
#if defined(TBB_MODE) && TBB_MODE == TBB_AUTO
  // The historical benchmark wrapper omits grain. Preserve truthful recorder
  // metadata without changing the ordinary benchmark's execution path.
  tbb::parallel_for(tbb::blocked_range<std::size_t>(0, tasks, grain),
      [&](const auto &range) {
        for (auto i = range.begin(); i < range.end(); ++i) body(i);
      }, tbb::auto_partitioner{});
#else
  ParallelFor(0, tasks, std::forward<F>(body), grain);
#endif
}

int main(int argc, char **argv) {
  using namespace scheduler_eval;
  namespace trace = oox::scheduler_trace;
  try {
    const auto threads = static_cast<std::size_t>(ExecutionThreads());
    const auto tasks = Argument(argc, argv, "--tasks", 256);
    const auto iterations = Argument(argc, argv, "--iterations", 3);
    const auto work = Argument(argc, argv, "--work", 200);
    const auto grain = Argument(argc, argv, "--grain", 1);
    const auto capacity = Argument(argc, argv, "--events-per-thread", 32768);
    std::string shape = "balanced", output;
    bool callbacks = false;
    for (int i = 1; i < argc; ++i) {
      const std::string_view key(argv[i]);
      if (key == "--callbacks") { callbacks = true; continue; }
      if (key != "--tasks" && key != "--iterations" && key != "--work" && key != "--grain" &&
          key != "--events-per-thread" && key != "--shape" && key != "--output")
        throw std::invalid_argument("unknown option: " + std::string(key));
      if (++i >= argc) throw std::invalid_argument("missing option value");
      if (key == "--shape") shape = argv[i];
      if (key == "--output") output = argv[i];
    }
    if (threads > 64 || !tasks || tasks > 1048576 || !iterations || iterations > 1000 ||
        !grain || work > 1000000 || tasks * iterations > 4194304 || output.empty())
      throw std::invalid_argument("require output, threads<=64, tasks<=1M, iterations<=1000, total callbacks<=4M, work<=1M, grain>0");
    if (shape != "balanced" && shape != "hyperbolic" && shape != "tail")
      throw std::invalid_argument("shape must be balanced, hyperbolic or tail (synthetic work, not SpMV)");
    // Refuse accidental overwrite. The Python runner additionally reserves a
    // fresh directory and never reuses a mode's output path.
    if (std::ifstream(output).good()) throw std::invalid_argument("output already exists");
    std::ofstream out(output);
    if (!out) throw std::runtime_error("cannot create trace output");
    Initialize();
    RunTraceLoop(tasks, [](std::size_t) {}, grain);
    auto visits = std::make_unique<std::atomic<unsigned>[]>(tasks);
    trace::Start(threads + 8, capacity);
    trace::Worker(0);
    for (std::size_t iteration = 0; iteration < iterations; ++iteration) {
      trace::Scope wave("wave", trace::NextId(), iteration, tasks);
      RunTraceLoop(tasks, [&](std::size_t i) {
#if (defined(TBB_MODE) && TBB_MODE == TBB_AUTO) || \
    (defined(EIGEN_MODE) && (EIGEN_MODE == EIGEN_STEALING || EIGEN_MODE == EIGEN_SHARING))
        // These adapters do not expose executed-range scopes. Record the
        // observed iteration, not a fabricated scheduler task or split.
        trace::Scope executed_iteration("chunk", trace::NextId(), i, i + 1);
#endif
        const auto body = [&] {
          const std::size_t units = shape == "hyperbolic" ? work * (1 + 64 / (i + 1))
              : shape == "tail" && i >= tasks * 3 / 4 ? work * 16 : work;
          for (std::size_t j = 0; j < units; ++j) CpuRelax();
          visits[i].fetch_add(1, std::memory_order_relaxed);
        };
        if (callbacks) {
          trace::Scope callback("callback", trace::NextId(), i, i + 1);
          body();
        } else body();
      }, grain);
    }
    bool correct = true;
    for (std::size_t i = 0; i < tasks; ++i)
      if (visits[i].load() != iterations) { correct = false; std::cerr << "visitation mismatch index=" << i << '\n'; break; }
    trace::Dump(out, ModeName(), shape, threads, tasks, iterations, work, grain, correct);
    out.flush();
    if (!out) throw std::runtime_error("trace output write failed");
    return correct ? 0 : 1;
  } catch (const std::exception &error) {
    std::cerr << "trace_scheduler: " << error.what() << '\n';
    return 2;
  }
}
