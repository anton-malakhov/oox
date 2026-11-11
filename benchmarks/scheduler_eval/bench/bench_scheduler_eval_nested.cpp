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

void NestedFiveSkew(benchmark::State &state, NestedFiveShape shape,
                    NestedFivePattern pattern) {
  const NestedFiveConfig config{
      shape, pattern, static_cast<std::size_t>(state.range(0))};
  std::vector<std::uint64_t> output(NestedFiveLeafCount);
  std::vector<std::uint64_t> expected(NestedFiveLeafCount);
  std::size_t work_steps = 0;
  // Flat reference: it does not repeat the nested traversal or its indexing.
  for (std::size_t index = 0; index < expected.size(); ++index) {
    expected[index] = NestedFiveLeafValue(index, config);
    work_steps += NestedFiveLeafSteps(index, config);
  }
  if (work_steps != NestedFiveWorkSteps) {
    state.SkipWithError("nested-five workload has inconsistent useful work");
    return;
  }
  SchedulerMetricsScope metrics(state);
  std::uint64_t checksum = 0;
  for (auto _ : state) {
    state.PauseTiming();
    std::fill(output.begin(), output.end(), std::uint64_t{0});
    state.ResumeTiming();
    NestedFive(config, output);
    benchmark::DoNotOptimize(output.data());
    benchmark::ClobberMemory();
    state.PauseTiming();
    bool correct = true;
    checksum = 0;
    for (std::size_t index = 0; index < output.size(); ++index) {
      correct &= output[index] == expected[index];
      checksum += output[index] ^ (std::uint64_t(index) * 0x9e3779b97f4a7c15ULL);
    }
    benchmark::DoNotOptimize(checksum);
    state.ResumeTiming();
    if (!correct) {
      state.SkipWithError("nested-five output differs from flat reference");
      break;
    }
  }
  state.SetItemsProcessed(state.iterations() * work_steps);
  state.counters["depth"] = 5;
  state.counters["leaves"] = NestedFiveLeafCount;
  state.counters["work_steps"] = work_steps;
  state.counters["ancestor_grain"] = config.ancestor_grain;
  state.counters["checksum_low32"] = checksum & 0xffffffffULL;
  state.counters["checksum_high32"] = checksum >> 32;
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

// Fixed dimensions and work are independent of requested worker count. Grain
// two changes only the second level; no callback is gated or blocked here.
BENCHMARK_CAPTURE(NestedFiveSkew, NarrowBalanced,
                  NestedFiveShape::NarrowAncestors, NestedFivePattern::Balanced)
    ->Setup(SetupNested)->ArgName("ancestor_grain")->Arg(1)->Arg(2)->UseRealTime();
BENCHMARK_CAPTURE(NestedFiveSkew, NarrowEarlyInner,
                  NestedFiveShape::NarrowAncestors, NestedFivePattern::EarlyInner)
    ->Setup(SetupNested)->ArgName("ancestor_grain")->Arg(1)->Arg(2)->UseRealTime();
BENCHMARK_CAPTURE(NestedFiveSkew, NarrowLateInner,
                  NestedFiveShape::NarrowAncestors, NestedFivePattern::LateInner)
    ->Setup(SetupNested)->ArgName("ancestor_grain")->Arg(1)->Arg(2)->UseRealTime();
BENCHMARK_CAPTURE(NestedFiveSkew, NarrowEarlyLeaf,
                  NestedFiveShape::NarrowAncestors, NestedFivePattern::EarlyLeaf)
    ->Setup(SetupNested)->ArgName("ancestor_grain")->Arg(1)->UseRealTime();
BENCHMARK_CAPTURE(NestedFiveSkew, NarrowLateLeaf,
                  NestedFiveShape::NarrowAncestors, NestedFivePattern::LateLeaf)
    ->Setup(SetupNested)->ArgName("ancestor_grain")->Arg(1)->UseRealTime();
BENCHMARK_CAPTURE(NestedFiveSkew, BatchedBalanced,
                  NestedFiveShape::BatchedAncestor, NestedFivePattern::Balanced)
    ->Setup(SetupNested)->ArgName("ancestor_grain")->Arg(1)->Arg(2)->UseRealTime();
BENCHMARK_CAPTURE(NestedFiveSkew, BatchedEarlyInner,
                  NestedFiveShape::BatchedAncestor, NestedFivePattern::EarlyInner)
    ->Setup(SetupNested)->ArgName("ancestor_grain")->Arg(1)->Arg(2)->UseRealTime();
BENCHMARK_CAPTURE(NestedFiveSkew, BatchedLateInner,
                  NestedFiveShape::BatchedAncestor, NestedFivePattern::LateInner)
    ->Setup(SetupNested)->ArgName("ancestor_grain")->Arg(1)->Arg(2)->UseRealTime();
BENCHMARK_CAPTURE(NestedFiveSkew, BatchedEarlyLeaf,
                  NestedFiveShape::BatchedAncestor, NestedFivePattern::EarlyLeaf)
    ->Setup(SetupNested)->ArgName("ancestor_grain")->Arg(1)->UseRealTime();
BENCHMARK_CAPTURE(NestedFiveSkew, BatchedLateLeaf,
                  NestedFiveShape::BatchedAncestor, NestedFivePattern::LateLeaf)
    ->Setup(SetupNested)->ArgName("ancestor_grain")->Arg(1)->UseRealTime();

} // namespace
