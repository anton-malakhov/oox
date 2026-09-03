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

BENCHMARK(MatrixMultiply)->Setup(SetupNested)->UseRealTime();
BENCHMARK(MatrixTranspose)->Setup(SetupNested)->UseRealTime();

} // namespace
