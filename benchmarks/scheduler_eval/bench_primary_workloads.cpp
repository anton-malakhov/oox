// SPDX-License-Identifier: Apache-2.0

#include "primary_workloads.h"
#include "scheduler_metrics.h"

#include <benchmark/benchmark.h>

namespace {

template <scheduler_eval::PointKind Kind>
void ConvexHull(benchmark::State &state) {
  scheduler_eval::SchedulerMetricsScope metrics(state);
  const auto points = scheduler_eval::MakePoints(Kind, state.range(0));
  for (auto _ : state)
    benchmark::DoNotOptimize(scheduler_eval::ConvexHullParallel(points));
  state.SetItemsProcessed(state.iterations() * points.size());
}

template <scheduler_eval::KeyKind Kind>
void RemoveDuplicates(benchmark::State &state) {
  scheduler_eval::SchedulerMetricsScope metrics(state);
  const auto keys = scheduler_eval::MakeKeys(Kind, state.range(0));
  for (auto _ : state)
    benchmark::DoNotOptimize(scheduler_eval::RemoveDuplicatesParallel(keys));
  state.SetItemsProcessed(state.iterations() * keys.size());
}

template <scheduler_eval::KeyKind Kind>
void RadixSort(benchmark::State &state) {
  scheduler_eval::SchedulerMetricsScope metrics(state);
  const auto keys = scheduler_eval::MakeKeys(Kind, state.range(0));
  for (auto _ : state)
    benchmark::DoNotOptimize(scheduler_eval::RadixSortParallel(keys));
  state.SetItemsProcessed(state.iterations() * keys.size());
}

template <scheduler_eval::KeyKind Kind>
void SampleSort(benchmark::State &state) {
  scheduler_eval::SchedulerMetricsScope metrics(state);
  const auto keys = scheduler_eval::MakeKeys(Kind, state.range(0));
  for (auto _ : state)
    benchmark::DoNotOptimize(scheduler_eval::SampleSortParallel(keys));
  state.SetItemsProcessed(state.iterations() * keys.size());
}

#define REGISTER_POINT(kind)                                                   \
  BENCHMARK_TEMPLATE(ConvexHull, scheduler_eval::PointKind::kind)              \
      ->RangeMultiplier(4)                                                     \
      ->Range(1 << 10, 1 << 18)                                                \
      ->UseRealTime()

#define REGISTER_KEYS(benchmark_name, kind)                                    \
  BENCHMARK_TEMPLATE(benchmark_name, scheduler_eval::KeyKind::kind)            \
      ->RangeMultiplier(4)                                                     \
      ->Range(1 << 10, 1 << 18)                                                \
      ->UseRealTime()

REGISTER_POINT(UniformSquare);
REGISTER_POINT(InDisk);
REGISTER_POINT(OnCircle);
REGISTER_POINT(Kuzmin);

#define REGISTER_KEY_FAMILY(kind)                                              \
  REGISTER_KEYS(RemoveDuplicates, kind);                                       \
  REGISTER_KEYS(RadixSort, kind);                                              \
  REGISTER_KEYS(SampleSort, kind)

REGISTER_KEY_FAMILY(Uniform);
REGISTER_KEY_FAMILY(Exponential);
REGISTER_KEY_FAMILY(DuplicateHeavy);
REGISTER_KEY_FAMILY(AlmostSorted);

} // namespace
