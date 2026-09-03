// SPDX-License-Identifier: Apache-2.0

#include "primary_workloads.h"
#include "scheduler_metrics.h"

#include <benchmark/benchmark.h>

namespace {

template <scheduler_eval::PointKind Kind>
void ConvexHull(benchmark::State &state) {
  const auto points = scheduler_eval::MakePoints(Kind, state.range(0));
  scheduler_eval::ConvexHullMetrics workload_metrics;
  benchmark::DoNotOptimize(
      scheduler_eval::ConvexHullParallel(points, &workload_metrics));
  scheduler_eval::SchedulerMetricsScope metrics(state);
  for (auto _ : state)
    benchmark::DoNotOptimize(scheduler_eval::ConvexHullParallel(points));
  state.counters["hull_vertices"] = workload_metrics.hull_vertices;
  state.counters["merge_passes"] = workload_metrics.merge_passes;
  state.SetItemsProcessed(state.iterations() * points.size());
}

template <scheduler_eval::KeyKind Kind>
void RemoveDuplicates(benchmark::State &state) {
  const auto keys = scheduler_eval::MakeKeys(Kind, state.range(0));
  scheduler_eval::DedupMetrics workload_metrics;
  benchmark::DoNotOptimize(
      scheduler_eval::RemoveDuplicatesParallel(keys, &workload_metrics));
  scheduler_eval::SchedulerMetricsScope metrics(state);
  for (auto _ : state)
    benchmark::DoNotOptimize(scheduler_eval::RemoveDuplicatesParallel(keys));
  state.counters["unique_items"] = workload_metrics.unique_items;
  state.counters["hash_probes_per_item"] =
      static_cast<double>(workload_metrics.hash_probes) / keys.size();
  state.counters["hash_load_factor"] =
      static_cast<double>(workload_metrics.unique_items) /
      static_cast<double>(workload_metrics.table_capacity);
  state.SetItemsProcessed(state.iterations() * keys.size());
}

template <scheduler_eval::KeyKind Kind>
void RadixSort(benchmark::State &state) {
  const auto keys = scheduler_eval::MakeKeys(Kind, state.range(0));
  scheduler_eval::RadixSortMetrics workload_metrics;
  benchmark::DoNotOptimize(
      scheduler_eval::RadixSortParallel(keys, &workload_metrics));
  scheduler_eval::SchedulerMetricsScope metrics(state);
  for (auto _ : state)
    benchmark::DoNotOptimize(scheduler_eval::RadixSortParallel(keys));
  state.counters["radix_passes"] = workload_metrics.passes;
  state.SetItemsProcessed(state.iterations() * keys.size());
}

template <scheduler_eval::KeyKind Kind>
void RadixSortPairs(benchmark::State &state) {
  const auto pairs = scheduler_eval::MakeKeyValues(Kind, state.range(0));
  scheduler_eval::RadixSortMetrics workload_metrics;
  benchmark::DoNotOptimize(
      scheduler_eval::RadixSortPairsParallel(pairs, &workload_metrics));
  scheduler_eval::SchedulerMetricsScope metrics(state);
  for (auto _ : state)
    benchmark::DoNotOptimize(scheduler_eval::RadixSortPairsParallel(pairs));
  state.counters["radix_passes"] = workload_metrics.passes;
  state.SetItemsProcessed(state.iterations() * pairs.size());
}

template <scheduler_eval::KeyKind Kind>
void SampleSort(benchmark::State &state) {
  const auto keys = scheduler_eval::MakeKeys(Kind, state.range(0));
  scheduler_eval::SampleSortMetrics workload_metrics;
  benchmark::DoNotOptimize(
      scheduler_eval::SampleSortParallel(keys, &workload_metrics));
  scheduler_eval::SchedulerMetricsScope metrics(state);
  for (auto _ : state)
    benchmark::DoNotOptimize(scheduler_eval::SampleSortParallel(keys));
  state.counters["sample_buckets"] = workload_metrics.buckets;
  state.counters["largest_bucket"] = workload_metrics.largest_bucket;
  state.counters["bucket_imbalance"] =
      static_cast<double>(workload_metrics.largest_bucket) /
      (static_cast<double>(keys.size()) / workload_metrics.buckets);
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
  REGISTER_KEYS(RadixSortPairs, kind);                                         \
  REGISTER_KEYS(SampleSort, kind)

REGISTER_KEY_FAMILY(Uniform);
REGISTER_KEY_FAMILY(Exponential);
REGISTER_KEY_FAMILY(DuplicateHeavy);
REGISTER_KEY_FAMILY(AlmostSorted);
REGISTER_KEY_FAMILY(ReverseSorted);

} // namespace
