// SPDX-License-Identifier: Apache-2.0

#include "common.h"
#include "synthetic_workloads.h"

#include <benchmark/benchmark.h>

#include <numeric>
#include <thread>
#include <vector>

namespace {

template <scheduler_eval::CostKind Kind>
void VariableCost(benchmark::State &state) {
  const auto costs = scheduler_eval::MakeIterationCosts(Kind, state.range(0));
  for (auto _ : state)
    benchmark::DoNotOptimize(scheduler_eval::RunCostLoop(costs));
  state.SetItemsProcessed(state.iterations() * costs.size());
}

#define REGISTER_COST(kind)                                                    \
  BENCHMARK_TEMPLATE(VariableCost, scheduler_eval::CostKind::kind)             \
      ->RangeMultiplier(4)                                                     \
      ->Range(1 << 10, 1 << 18)                                                \
      ->UseRealTime()

REGISTER_COST(Constant);
REGISTER_COST(Uniform);
REGISTER_COST(Exponential);
REGISTER_COST(Pareto);
REGISTER_COST(Linear);
REGISTER_COST(Clustered);
REGISTER_COST(Periodic);
REGISTER_COST(Shuffled);
REGISTER_COST(PhaseChanging);

void CompetingLoops(benchmark::State &state) {
  const auto costs = scheduler_eval::MakeIterationCosts(
      scheduler_eval::CostKind::Clustered, state.range(0));
  for (auto _ : state) {
    std::uint64_t first = 0;
    std::thread competitor([&] { first = scheduler_eval::RunCostLoop(costs); });
    auto second = scheduler_eval::RunCostLoop(costs);
    competitor.join();
    benchmark::DoNotOptimize(first);
    benchmark::DoNotOptimize(second);
  }
  state.SetItemsProcessed(state.iterations() * costs.size() * 2);
}

template <bool ParallelTouch> void FirstTouch(benchmark::State &state) {
  std::vector<std::uint64_t> data(state.range(0)), output(data.size());
  for (auto _ : state) {
    state.PauseTiming();
    if constexpr (ParallelTouch)
      ParallelFor(0, data.size(), [&](std::size_t i) { data[i] = i + 1; });
    else
      std::iota(data.begin(), data.end(), std::uint64_t{1});
    state.ResumeTiming();
    ParallelFor(0, data.size(),
                [&](std::size_t i) { output[i] = data[i] * 3; });
    benchmark::DoNotOptimize(output.data());
  }
  state.SetBytesProcessed(state.iterations() * data.size() * sizeof(data[0]));
}

BENCHMARK(CompetingLoops)
    ->RangeMultiplier(4)
    ->Range(1 << 10, 1 << 18)
    ->UseRealTime();
BENCHMARK_TEMPLATE(FirstTouch, false)
    ->RangeMultiplier(4)
    ->Range(1 << 14, 1 << 24)
    ->UseRealTime();
BENCHMARK_TEMPLATE(FirstTouch, true)
    ->RangeMultiplier(4)
    ->Range(1 << 14, 1 << 24)
    ->UseRealTime();

} // namespace
