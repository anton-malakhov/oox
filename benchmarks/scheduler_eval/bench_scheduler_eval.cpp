// SPDX-License-Identifier: Apache-2.0

#include "common.h"
#include "workloads.h"

#include <benchmark/benchmark.h>

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <vector>

namespace {

using namespace scheduler_eval;

void Setup(const benchmark::State &) { Initialize(); }

void Launch(benchmark::State &state) {
  for (auto _ : state)
    ParallelFor(0, static_cast<std::size_t>(state.range(0)),
                [](std::size_t i) { benchmark::DoNotOptimize(i); });
}

enum class SpinPayload { Relax, Atomic, DistributedRead, ThreadLocal };

struct alignas(hardware_constructive_interference_size) IsolatedValue {
  std::uint64_t value{1};
};

template <SpinPayload Payload> void Spin(benchmark::State &state) {
  const auto tasks = static_cast<std::size_t>(state.range(0));
  const auto work = static_cast<std::size_t>(state.range(1));
  const auto calls = static_cast<std::size_t>(state.range(2));
  alignas(hardware_constructive_interference_size)
      std::atomic<std::uint64_t> shared{1};
  std::vector<IsolatedValue> distributed(tasks);
  for (auto _ : state) {
    for (std::size_t call = 0; call < calls; ++call) {
      ParallelFor(0, tasks, [&](std::size_t task) {
        if constexpr (Payload == SpinPayload::Relax) {
          for (std::size_t i = 0; i < work; ++i)
            CpuRelax();
        } else if constexpr (Payload == SpinPayload::Atomic) {
          std::uint64_t value = 0;
          for (std::size_t i = 0; i < work; ++i)
            value += shared.load(std::memory_order_relaxed);
          benchmark::DoNotOptimize(value);
        } else if constexpr (Payload == SpinPayload::DistributedRead) {
          std::uint64_t value = 0;
          const volatile auto *source = &distributed[task].value;
          for (std::size_t i = 0; i < work; ++i)
            value = *source;
          benchmark::DoNotOptimize(value);
        } else {
          static thread_local std::uint64_t local = 1;
          volatile std::uint64_t value = 0;
          for (std::size_t i = 0; i < work; ++i)
            value = local;
          benchmark::DoNotOptimize(value);
        }
      });
    }
  }
  state.SetItemsProcessed(state.iterations() * tasks * calls);
}

void Reduce(benchmark::State &state) {
  const auto size = (static_cast<std::size_t>(GetNumThreads()) << 19) +
                    (static_cast<std::size_t>(GetNumThreads()) << 3) + 3;
  const auto block_size = static_cast<std::size_t>(state.range(0)) +
                          static_cast<std::size_t>(GetNumThreads()) + 3;
  std::vector<double> data(size, 1.0);
  for (auto _ : state) {
    auto sum = BlockedReduce(data, block_size);
    benchmark::DoNotOptimize(sum);
  }
  state.SetItemsProcessed(state.iterations() * size);
}

void Scan(benchmark::State &state) {
  const auto size = std::size_t{1} << state.range(0);
  std::vector<std::uint64_t> data(size);
  for (auto _ : state) {
    state.PauseTiming();
    std::fill(data.begin(), data.end(), 1);
    state.ResumeTiming();
    ExclusiveScan(data);
    benchmark::DoNotOptimize(data.data());
  }
  state.SetItemsProcessed(state.iterations() * size);
}

template <SparseKind Kind> void SpmvBenchmark(benchmark::State &state) {
  const auto rows = (static_cast<std::size_t>(GetNumThreads()) << 9) +
                    (static_cast<std::size_t>(GetNumThreads()) << 4) + 7;
  const auto columns = static_cast<std::size_t>(state.range(0)) +
                       (static_cast<std::size_t>(GetNumThreads()) << 2) + 3;
  const auto matrix = MakeSparseMatrix(rows, columns, Kind);
  std::vector<double> input(columns, 1.0), output;
  for (auto _ : state) {
    Spmv(matrix, input, output);
    benchmark::DoNotOptimize(output.data());
  }
  state.SetItemsProcessed(state.iterations() * matrix.values.size());
}

#define SCHEDULER_SPIN_ARGS(scale)                                             \
  ->ArgNames({"tasks", "work", "calls"})                                       \
      ->Args({1 << 10, (1 << 10) * scale, 1})                                  \
      ->Args({GetNumThreads(), (1 << 20) * scale, 1})                          \
      ->Args({1 << 13, (1 << 13) * scale, 1})                                  \
      ->Args({1 << 16, (1 << 10) * scale, 1})                                  \
      ->Args({GetNumThreads(), 1, 1024})                                       \
      ->Args({1 << 20, 1, 1})

BENCHMARK(Launch)
    ->Setup(Setup)
    ->RangeMultiplier(4)
    ->Range(64, 1 << 18)
    ->UseRealTime();
BENCHMARK_TEMPLATE(Spin, SpinPayload::Relax)
    ->Setup(Setup) SCHEDULER_SPIN_ARGS(1)
    ->UseRealTime();
BENCHMARK_TEMPLATE(Spin, SpinPayload::Atomic)
    ->Setup(Setup) SCHEDULER_SPIN_ARGS(32)
    ->UseRealTime();
BENCHMARK_TEMPLATE(Spin, SpinPayload::DistributedRead)
    ->Setup(Setup) SCHEDULER_SPIN_ARGS(32)
    ->UseRealTime();
BENCHMARK_TEMPLATE(Spin, SpinPayload::ThreadLocal)
    ->Setup(Setup) SCHEDULER_SPIN_ARGS(32)
    ->UseRealTime();
BENCHMARK(Reduce)
    ->Setup(Setup)
    ->RangeMultiplier(2)
    ->Range(1 << 12, 1 << 19)
    ->UseRealTime();
BENCHMARK(Scan)->Setup(Setup)->DenseRange(10, 24, 2)->UseRealTime();
BENCHMARK_TEMPLATE(SpmvBenchmark, SparseKind::Balanced)
    ->Setup(Setup)
    ->RangeMultiplier(2)
    ->Range(1 << 12, 1 << 17)
    ->UseRealTime();
BENCHMARK_TEMPLATE(SpmvBenchmark, SparseKind::Hyperbolic)
    ->Setup(Setup)
    ->RangeMultiplier(2)
    ->Range(1 << 12, 1 << 17)
    ->UseRealTime();
BENCHMARK_TEMPLATE(SpmvBenchmark, SparseKind::Triangle)
    ->Setup(Setup)
    ->RangeMultiplier(2)
    ->Range(1 << 12, 1 << 17)
    ->UseRealTime();
} // namespace

BENCHMARK_MAIN();
