// SPDX-License-Identifier: Apache-2.0

#include "common.h"
#include "scheduler_metrics.h"
#include "synthetic_workloads.h"

#include <benchmark/benchmark.h>

#include <numeric>
#include <memory>
#include <thread>
#include <vector>
#if defined(__unix__) || defined(__APPLE__)
#include <sys/mman.h>
#endif

namespace {

template <scheduler_eval::CostKind Kind>
void VariableCost(benchmark::State &state) {
  scheduler_eval::SchedulerMetricsScope metrics(state);
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
  scheduler_eval::SchedulerMetricsScope metrics(state);
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

void OversubscribedLoops(benchmark::State &state) {
  const auto costs = scheduler_eval::MakeIterationCosts(
      scheduler_eval::CostKind::Clustered, state.range(0));
  const auto clients = static_cast<std::size_t>(state.range(1));
  const auto expected = scheduler_eval::RunCostLoopSerial(costs);
  scheduler_eval::SchedulerMetricsScope metrics(state);
  for (auto _ : state) {
    std::vector<std::uint64_t> results(clients);
    std::vector<std::thread> callers;
    for (std::size_t i = 0; i < clients; ++i)
      callers.emplace_back([&, i] {
        results[i] = scheduler_eval::RunCostLoop(costs);
      });
    for (auto &caller : callers)
      caller.join();
    if (!std::all_of(results.begin(), results.end(),
                     [&](auto result) { return result == expected; }))
      state.SkipWithError("concurrent loops differ from serial checksum");
    benchmark::DoNotOptimize(results.data());
  }
  state.SetItemsProcessed(state.iterations() * clients * costs.size());
}

#if defined(EIGEN_MODE) || defined(OOX_TASK_MODE)
void ChangingWorkerAvailability(benchmark::State &state) {
#ifdef OOX_TASK_MODE
  auto &pool = oox::internal::get_eigen_pool();
#else
  auto &pool = EigenPool();
#endif
  const auto unavailable = pool.NumThreads() / 2;
  if (pool.NumThreads() < 2) {
    state.SkipWithError("worker availability requires at least two workers");
    return;
  }
  const auto costs = scheduler_eval::MakeIterationCosts(
      scheduler_eval::CostKind::PhaseChanging, state.range(0));
  const auto expected = scheduler_eval::RunCostLoopSerial(costs);
  scheduler_eval::SchedulerMetricsScope metrics(state);
  for (auto _ : state) {
    state.PauseTiming();
    std::atomic<std::size_t> started{0}, finished{0};
    std::atomic<bool> release{false};
    for (std::size_t i = 0; i < unavailable; ++i)
      pool.Schedule(oox::detail::eigen_pool::MakeTask([&] {
        started.fetch_add(1);
        while (!release.load())
          std::this_thread::yield();
        finished.fetch_add(1);
      }));
    while (started.load() != unavailable)
      std::this_thread::yield();
    state.ResumeTiming();
    // Workers return during the loop; a separate controller ensures progress.
    std::thread controller([&] {
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
      release.store(true);
    });
    const auto result = scheduler_eval::RunCostLoop(costs);
    controller.join();
    while (finished.load() != unavailable)
      std::this_thread::yield();
    if (result != expected)
      state.SkipWithError("availability loop differs from serial checksum");
    benchmark::DoNotOptimize(result);
  }
  state.counters["temporarily_unavailable_workers"] = unavailable;
  state.SetItemsProcessed(state.iterations() * costs.size());
}

BENCHMARK(ChangingWorkerAvailability)
    ->Arg(1 << 12)->Arg(1 << 18)->UseRealTime();
#endif

#ifndef RAPID_START_MODE
BENCHMARK(OversubscribedLoops)
    ->Args({1 << 12, 2})->Args({1 << 12, 4})->Args({1 << 12, 8})
    ->UseRealTime();
#endif

template <bool ParallelTouch> void FirstTouch(benchmark::State &state) {
#if defined(__unix__) || defined(__APPLE__)
  scheduler_eval::SchedulerMetricsScope metrics(state);
  const auto size = static_cast<std::size_t>(state.range(0));
  const auto bytes = size * sizeof(std::uint64_t);
  std::vector<std::uint64_t> output(size);
  for (auto _ : state) {
    state.PauseTiming();
    auto *address = static_cast<std::uint64_t *>(mmap(
        nullptr, bytes, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0));
    if (address == MAP_FAILED) {
      state.ResumeTiming();
      state.SkipWithError("unable to map fresh pages for first-touch experiment");
      break;
    }
    const auto unmap = [bytes](std::uint64_t *p) { munmap(p, bytes); };
    std::unique_ptr<std::uint64_t, decltype(unmap)> data(address, unmap);
    if constexpr (ParallelTouch)
      EvalParallelFor(0, size, [&](std::size_t i) { data.get()[i] = i + 1; });
    else
      std::iota(data.get(), data.get() + size, std::uint64_t{1});
    state.ResumeTiming();
    EvalParallelFor(0, size,
                [&](std::size_t i) { output[i] = data.get()[i] * 3; });
    benchmark::DoNotOptimize(output.data());
    state.PauseTiming();
    bool valid = true;
    for (std::size_t i = 0; i < size; ++i)
      valid &= output[i] == (i + 1) * 3;
    data.reset();
    state.ResumeTiming();
    if (!valid)
      state.SkipWithError("first-touch output differs from serial formula");
  }
  state.counters["fresh_anonymous_mapping"] = 1;
  state.SetBytesProcessed(state.iterations() * bytes);
#else
  state.SkipWithError("first-touch experiment requires anonymous POSIX mappings");
#endif
}

#ifndef RAPID_START_MODE
BENCHMARK(CompetingLoops)
    ->RangeMultiplier(4)
    ->Range(1 << 10, 1 << 18)
    ->UseRealTime();
#endif
BENCHMARK_TEMPLATE(FirstTouch, false)
    ->RangeMultiplier(4)
    ->Range(1 << 14, 1 << 24)
    ->UseRealTime();
BENCHMARK_TEMPLATE(FirstTouch, true)
    ->RangeMultiplier(4)
    ->Range(1 << 14, 1 << 24)
    ->UseRealTime();

} // namespace
