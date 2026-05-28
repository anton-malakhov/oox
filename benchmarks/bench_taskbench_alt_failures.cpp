#include "taskbench/taskbench_graph.h"

#include <benchmark/benchmark.h>

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <memory>
#include <span>
#include <stdexcept>
#include <vector>

#if HAVE_TBB
#ifndef TBB_USE_ASSERT
#define TBB_USE_ASSERT 0
#endif
#include <oneapi/tbb/flow_graph.h>
#endif

#if HAVE_TF
#include <taskflow/taskflow.hpp>
#endif

namespace tb = oox_bench::taskbench;

namespace {

struct injected_taskbench_failure final : std::exception {
  const char* what() const noexcept override {
    return "injected taskbench failure";
  }
};

struct FailureStats {
  std::int64_t completed = 0;
  std::int64_t failed = 0;
};

tb::Config make_config(int height, int width) {
  tb::Config cfg;
  cfg.height = height;
  cfg.width = width;
  cfg.graphs = 1;
  cfg.pattern = tb::Pattern::Stencil;
  cfg.kernel = tb::Kernel::Empty;
  cfg.iterations = 0;
  cfg.output_bytes = 0;
  cfg.scratch_bytes = 0;
  cfg.validate = false;
  return cfg;
}

std::size_t task_index(const tb::Graph& graph, int graph_id, int row, int col) {
  const auto width = static_cast<std::size_t>(graph.width());
  return (static_cast<std::size_t>(graph_id) * static_cast<std::size_t>(graph.config().height) +
          static_cast<std::size_t>(row)) *
             width +
         static_cast<std::size_t>(col);
}

tb::Token execute_point(const tb::Graph& graph,
                        const std::vector<tb::Token>& tokens,
                        int graph_id,
                        int row,
                        int col) {
  std::vector<tb::Token> inputs;
  inputs.reserve(OOX_TASKBENCH_MAX_DEPS);
  for (int dep : graph.deps(row, col)) {
    inputs.push_back(tokens[task_index(graph, graph_id, row - 1, dep)]);
  }
  return graph.execute_point(graph_id, row, col, std::span<const tb::Token>(inputs));
}

enum class FailurePosition {
  NearStart,
  Middle,
  NearEnd,
};

int failure_row(int height, FailurePosition position) {
  if (position == FailurePosition::Middle) {
    return height / 2;
  }
  int offset = height / 8;
  if (offset < 1) {
    offset = 1;
  }
  int row = position == FailurePosition::NearStart ? offset : height - offset;
  if (row < 1) {
    row = 1;
  }
  if (row >= height) {
    row = height - 1;
  }
  return row;
}

#if HAVE_TBB

FailureStats run_tbb_flow_throw(int height, int width, FailurePosition position) {
  tb::Graph graph(make_config(height, width));
  const auto total_tasks = static_cast<std::size_t>(graph.task_count());
  const int fail_row = failure_row(height, position);
  const int fail_col = width / 2;

  oneapi::tbb::flow::graph flow;
  using node_t = oneapi::tbb::flow::continue_node<oneapi::tbb::flow::continue_msg>;
  std::vector<tb::Token> tokens(total_tasks);
  std::vector<std::unique_ptr<node_t>> nodes(total_tasks);
  std::vector<bool> has_predecessor(total_tasks, false);
  std::atomic<std::int64_t> completed{0};

  for (int row = 0; row < height; ++row) {
    for (int col = 0; col < width; ++col) {
      const auto idx = task_index(graph, 0, row, col);
      nodes[idx] = std::make_unique<node_t>(flow, [&, row, col, idx](const oneapi::tbb::flow::continue_msg&) {
        if (row == fail_row && col == fail_col) {
          throw injected_taskbench_failure{};
        }
        tokens[idx] = execute_point(graph, tokens, 0, row, col);
        completed.fetch_add(1, std::memory_order_relaxed);
        return oneapi::tbb::flow::continue_msg{};
      });
    }
  }

  for (int row = 1; row < height; ++row) {
    for (int col = 0; col < width; ++col) {
      const auto curr = task_index(graph, 0, row, col);
      for (int dep : graph.deps(row, col)) {
        oneapi::tbb::flow::make_edge(*nodes[task_index(graph, 0, row - 1, dep)], *nodes[curr]);
        has_predecessor[curr] = true;
      }
    }
  }

  for (std::size_t idx = 0; idx < total_tasks; ++idx) {
    if (!has_predecessor[idx]) {
      nodes[idx]->try_put(oneapi::tbb::flow::continue_msg{});
    }
  }

  FailureStats stats;
  try {
    flow.wait_for_all();
  } catch (const injected_taskbench_failure&) {
    stats.failed = 1;
  }
  stats.completed = completed.load(std::memory_order_relaxed);
  return stats;
}

FailureStats run_tbb_flow_cancel(int height, int width, FailurePosition position) {
  tb::Graph graph(make_config(height, width));
  const auto total_tasks = static_cast<std::size_t>(graph.task_count());
  const int fail_row = failure_row(height, position);
  const int fail_col = width / 2;

  oneapi::tbb::flow::graph flow;
  using node_t = oneapi::tbb::flow::continue_node<oneapi::tbb::flow::continue_msg>;
  std::vector<tb::Token> tokens(total_tasks);
  std::vector<std::unique_ptr<node_t>> nodes(total_tasks);
  std::vector<bool> has_predecessor(total_tasks, false);
  std::atomic<std::int64_t> completed{0};
  std::atomic<std::int64_t> cancelled{0};

  for (int row = 0; row < height; ++row) {
    for (int col = 0; col < width; ++col) {
      const auto idx = task_index(graph, 0, row, col);
      nodes[idx] = std::make_unique<node_t>(flow, [&, row, col, idx](const oneapi::tbb::flow::continue_msg&) {
        if (row == fail_row && col == fail_col) {
          cancelled.fetch_add(1, std::memory_order_relaxed);
          flow.cancel();
          return oneapi::tbb::flow::continue_msg{};
        }
        tokens[idx] = execute_point(graph, tokens, 0, row, col);
        completed.fetch_add(1, std::memory_order_relaxed);
        return oneapi::tbb::flow::continue_msg{};
      });
    }
  }

  for (int row = 1; row < height; ++row) {
    for (int col = 0; col < width; ++col) {
      const auto curr = task_index(graph, 0, row, col);
      for (int dep : graph.deps(row, col)) {
        oneapi::tbb::flow::make_edge(*nodes[task_index(graph, 0, row - 1, dep)], *nodes[curr]);
        has_predecessor[curr] = true;
      }
    }
  }

  for (std::size_t idx = 0; idx < total_tasks; ++idx) {
    if (!has_predecessor[idx]) {
      nodes[idx]->try_put(oneapi::tbb::flow::continue_msg{});
    }
  }
  flow.wait_for_all();

  FailureStats stats;
  stats.completed = completed.load(std::memory_order_relaxed);
  stats.failed = cancelled.load(std::memory_order_relaxed);
  return stats;
}

void run_tbb_flow_throw_benchmark(benchmark::State& state, FailurePosition position, const char* error) {
  const int height = static_cast<int>(state.range(0));
  const int width = static_cast<int>(state.range(1));
  std::int64_t completed = 0;
  std::int64_t failed = 0;
  for (auto _ : state) {
    const auto stats = run_tbb_flow_throw(height, width, position);
    if (stats.failed == 0) {
      state.SkipWithError(error);
      break;
    }
    completed += stats.completed;
    failed += stats.failed;
  }
  state.SetItemsProcessed(state.iterations() * static_cast<std::int64_t>(height) * width);
  state.counters["completed"] = static_cast<double>(completed);
  state.counters["failed"] = static_cast<double>(failed);
}

void BM_TbbFlow_EarlyThrow(benchmark::State& state) {
  run_tbb_flow_throw_benchmark(state,
                               FailurePosition::NearStart,
                               "TBB flow graph did not propagate the early throw");
}

void BM_TbbFlow_MidThrow(benchmark::State& state) {
  run_tbb_flow_throw_benchmark(state,
                               FailurePosition::Middle,
                               "TBB flow graph did not propagate the middle throw");
}

void BM_TbbFlow_LateThrow(benchmark::State& state) {
  run_tbb_flow_throw_benchmark(state,
                               FailurePosition::NearEnd,
                               "TBB flow graph did not propagate the late throw");
}

void run_tbb_flow_cancel_benchmark(benchmark::State& state, FailurePosition position, const char* error) {
  const int height = static_cast<int>(state.range(0));
  const int width = static_cast<int>(state.range(1));
  std::int64_t completed = 0;
  std::int64_t failed = 0;
  for (auto _ : state) {
    const auto stats = run_tbb_flow_cancel(height, width, position);
    if (stats.failed == 0) {
      state.SkipWithError(error);
      break;
    }
    completed += stats.completed;
    failed += stats.failed;
  }
  state.SetItemsProcessed(state.iterations() * static_cast<std::int64_t>(height) * width);
  state.counters["completed"] = static_cast<double>(completed);
  state.counters["failed"] = static_cast<double>(failed);
}

void BM_TbbFlow_EarlyCancel(benchmark::State& state) {
  run_tbb_flow_cancel_benchmark(state,
                                FailurePosition::NearStart,
                                "TBB flow graph did not run the early cancel task");
}

void BM_TbbFlow_MidCancel(benchmark::State& state) {
  run_tbb_flow_cancel_benchmark(state,
                                FailurePosition::Middle,
                                "TBB flow graph did not run the middle cancel task");
}

void BM_TbbFlow_LateCancel(benchmark::State& state) {
  run_tbb_flow_cancel_benchmark(state,
                                FailurePosition::NearEnd,
                                "TBB flow graph did not run the late cancel task");
}

#endif

#if HAVE_TF

FailureStats run_taskflow_throw(int height, int width, FailurePosition position) {
  tb::Graph graph(make_config(height, width));
  const auto total_tasks = static_cast<std::size_t>(graph.task_count());
  const int fail_row = failure_row(height, position);
  const int fail_col = width / 2;

  std::vector<tb::Token> tokens(total_tasks);
  std::vector<tf::Task> tasks(total_tasks);
  std::atomic<std::int64_t> completed{0};
  tf::Taskflow flow("taskbench-failure");

  for (int row = 0; row < height; ++row) {
    for (int col = 0; col < width; ++col) {
      const auto idx = task_index(graph, 0, row, col);
      tasks[idx] = flow.emplace([&, row, col, idx] {
        if (row == fail_row && col == fail_col) {
          throw injected_taskbench_failure{};
        }
        tokens[idx] = execute_point(graph, tokens, 0, row, col);
        completed.fetch_add(1, std::memory_order_relaxed);
      });
    }
  }

  for (int row = 1; row < height; ++row) {
    for (int col = 0; col < width; ++col) {
      const auto curr = task_index(graph, 0, row, col);
      for (int dep : graph.deps(row, col)) {
        tasks[task_index(graph, 0, row - 1, dep)].precede(tasks[curr]);
      }
    }
  }

  FailureStats stats;
  tf::Executor executor;
  try {
    executor.run(flow).get();
  } catch (const injected_taskbench_failure&) {
    stats.failed = 1;
  }
  stats.completed = completed.load(std::memory_order_relaxed);
  return stats;
}

void run_taskflow_throw_benchmark(benchmark::State& state, FailurePosition position, const char* error) {
  const int height = static_cast<int>(state.range(0));
  const int width = static_cast<int>(state.range(1));
  std::int64_t completed = 0;
  std::int64_t failed = 0;
  for (auto _ : state) {
    const auto stats = run_taskflow_throw(height, width, position);
    if (stats.failed == 0) {
      state.SkipWithError(error);
      break;
    }
    completed += stats.completed;
    failed += stats.failed;
  }
  state.SetItemsProcessed(state.iterations() * static_cast<std::int64_t>(height) * width);
  state.counters["completed"] = static_cast<double>(completed);
  state.counters["failed"] = static_cast<double>(failed);
}

void BM_Taskflow_EarlyThrow(benchmark::State& state) {
  run_taskflow_throw_benchmark(state, FailurePosition::NearStart, "Taskflow did not propagate the early throw");
}

void BM_Taskflow_MidThrow(benchmark::State& state) {
  run_taskflow_throw_benchmark(state, FailurePosition::Middle, "Taskflow did not propagate the middle throw");
}

void BM_Taskflow_LateThrow(benchmark::State& state) {
  run_taskflow_throw_benchmark(state, FailurePosition::NearEnd, "Taskflow did not propagate the late throw");
}

#endif

} // namespace

#if HAVE_TBB
BENCHMARK(BM_TbbFlow_EarlyThrow)
    ->UseRealTime()
    ->Unit(benchmark::kMillisecond)
    ->Args({128, 16})
    ->Args({128, 32})
    ->Args({128, 64})
    ->Args({512, 32})
    ->Args({512, 64})
    ->Args({2048, 64})
    ;

BENCHMARK(BM_TbbFlow_MidThrow)
    ->UseRealTime()
    ->Unit(benchmark::kMillisecond)
    ->Args({128, 16})
    ->Args({128, 32})
    ->Args({128, 64})
    ->Args({512, 32})
    ->Args({512, 64})
    ->Args({2048, 64})
    ;

BENCHMARK(BM_TbbFlow_LateThrow)
    ->UseRealTime()
    ->Unit(benchmark::kMillisecond)
    ->Args({128, 16})
    ->Args({128, 32})
    ->Args({128, 64})
    ->Args({512, 32})
    ->Args({512, 64})
    ->Args({2048, 64})
    ;

BENCHMARK(BM_TbbFlow_EarlyCancel)
    ->UseRealTime()
    ->Unit(benchmark::kMillisecond)
    ->Args({128, 16})
    ->Args({128, 32})
    ->Args({128, 64})
    ->Args({512, 32})
    ->Args({512, 64})
    ->Args({2048, 64})
    ;

BENCHMARK(BM_TbbFlow_MidCancel)
    ->UseRealTime()
    ->Unit(benchmark::kMillisecond)
    ->Args({128, 16})
    ->Args({128, 32})
    ->Args({128, 64})
    ->Args({512, 32})
    ->Args({512, 64})
    ->Args({2048, 64})
    ;

BENCHMARK(BM_TbbFlow_LateCancel)
    ->UseRealTime()
    ->Unit(benchmark::kMillisecond)
    ->Args({128, 16})
    ->Args({128, 32})
    ->Args({128, 64})
    ->Args({512, 32})
    ->Args({512, 64})
    ->Args({2048, 64})
    ;
#endif

#if HAVE_TF
BENCHMARK(BM_Taskflow_EarlyThrow)
    ->UseRealTime()
    ->Unit(benchmark::kMillisecond)
    ->Args({128, 16})
    ->Args({128, 32})
    ->Args({128, 64})
    ->Args({512, 32})
    ->Args({512, 64})
    ->Args({2048, 64})
    ;

BENCHMARK(BM_Taskflow_MidThrow)
    ->UseRealTime()
    ->Unit(benchmark::kMillisecond)
    ->Args({128, 16})
    ->Args({128, 32})
    ->Args({128, 64})
    ->Args({512, 32})
    ->Args({512, 64})
    ->Args({2048, 64})
    ;

BENCHMARK(BM_Taskflow_LateThrow)
    ->UseRealTime()
    ->Unit(benchmark::kMillisecond)
    ->Args({128, 16})
    ->Args({128, 32})
    ->Args({128, 64})
    ->Args({512, 32})
    ->Args({512, 64})
    ->Args({2048, 64})
    ;
#endif

BENCHMARK_MAIN();
