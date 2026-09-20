// SPDX-License-Identifier: Apache-2.0

#include "common.h"
#include "nested_workloads.h"
#include "scheduler_metrics.h"

#include <benchmark/benchmark.h>

namespace {

using namespace scheduler_eval;

void SetupNested(const benchmark::State &) { Initialize(); }

void MatrixMultiply(benchmark::State &state) {
  SchedulerMetricsScope metrics(state);
  const auto size = (static_cast<std::size_t>(GetNumThreads()) << 3) +
                    static_cast<std::size_t>(GetNumThreads()) + 7;
  auto left = MakeDenseMatrix(size, size);
  auto right = MakeDenseMatrix(size, size);
  DenseMatrix output(size, size);
  for (auto _ : state) {
    Multiply(left, right, output);
    benchmark::DoNotOptimize(output.values.data());
  }
  state.SetItemsProcessed(state.iterations() * size * size * size);
}

void MatrixTranspose(benchmark::State &state) {
  SchedulerMetricsScope metrics(state);
  const auto size = (static_cast<std::size_t>(GetNumThreads()) << 4) +
                    static_cast<std::size_t>(GetNumThreads());
  auto input = MakeDenseMatrix(size, size);
  DenseMatrix output(size, size);
  for (auto _ : state) {
    Transpose(input, output);
    benchmark::DoNotOptimize(output.values.data());
  }
  state.SetItemsProcessed(state.iterations() * size * size);
}

void DeepNested(benchmark::State &state) {
  const auto depth = static_cast<std::size_t>(state.range(0));
  const auto fanout = static_cast<std::size_t>(state.range(1));
  const auto work = static_cast<std::size_t>(state.range(2));
  std::vector<std::uint64_t> output(1 << 16);
  SchedulerMetricsScope metrics(state);
  for (auto _ : state) {
    DeepNestedFor(0, output.size(), depth, fanout, [&](std::size_t i) {
      std::uint64_t value = i + 1;
      for (std::size_t step = 0; step < work; ++step)
        value = (value ^ (value >> 27)) * 0x3c79ac492ba7b653ULL + step;
      output[i] = value;
    });
    benchmark::DoNotOptimize(output.data());
    benchmark::ClobberMemory();
  }
  state.SetItemsProcessed(state.iterations() * output.size());
  state.counters["elements"] = output.size();
}

BENCHMARK(DeepNested)
    ->Setup(SetupNested)
    ->ArgNames({"depth", "fanout", "work"})
    ->Args({2, 4, 1})->Args({4, 4, 1})->Args({8, 4, 1})
    ->Args({2, 2, 64})->Args({4, 2, 64})->Args({8, 2, 64})
    ->Args({16, 2, 64})
    ->UseRealTime();

BENCHMARK(MatrixMultiply)->Setup(SetupNested)->UseRealTime();
BENCHMARK(MatrixTranspose)->Setup(SetupNested)->UseRealTime();

} // namespace
