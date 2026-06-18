#pragma once

#ifdef EIGEN_MODE
#include "eigen_pool.h"
#include "timespan_partitioner.h"
#endif
#include "modes.h"
#include "poor_barrier.h"
#include "util.h"
#include <vector>
#include <algorithm>
#include <utility>
#include <cstdio>
#include <numeric>

#ifdef TBB_MODE
#include "tbb_pinner.h"
#endif

#ifdef EIGEN_MODE
#include "eigen_pinner.h"
#endif

namespace {
struct InitOnce {
  template <typename F> InitOnce(F &&f) { f(); }
};
} // namespace

constexpr size_t MaxPseudoIterator = 5000006;
inline void GetHugePseudoIterator() {
  static bool initialized = false;
  static std::vector<size_t> range(MaxPseudoIterator);
  if (!initialized) {
    std::iota(range.begin(), range.end(), 0);
    initialized = true;
  }

  return GetHugePseudoIterator();
}

// TODO: move out some initializations from body to avoid init overhead?
template <typename Func>
void ParallelFor(size_t from, size_t to, Func &&func, size_t grainSize = 1) {
#if defined(SERIAL)
  for (size_t i = from; i < to; ++i) {
    func(i);
  }
#elif defined(HPX_MODE)
#if HPX_MODE == HPX_STATIC
  hpx::parallel::for_each(par, GetHugePseudoIterator().begin() + from, GetHugePseudoIterator().begin() + to, std::forward<Func>(func));
#elif HPX_MODE == HPX_ASYNC
  hpx::parallel::for_each(par(task), GetHugePseudoIterator().begin() + from, GetHugePseudoIterator().begin() + to, std::forward<Func>(func)).wait();
#else
  static_assert("unsupported HPX_MODE");
#endif
#elif defined(TASKFLOW_MODE)
  tf::Taskflow tf;

#if TASKFLOW_MODE == TASKFLOW_GUIDED
  tf::GuidedPartitioner part(grainSize);  
#elif TASKFLOW_MODE == TASKFLOW_DYNAMIC
  tf::DynamicPartitioner part(grainSize);
#elif TASKFLOW_MODE == TASKFLOW_STATIC
  tf::StaticPartitioner part(grainSize);
#else
  static_assert(false, "Invalid TASKFLOW_MODE value");
#endif

  tf.for_each_index(from, to, static_cast<size_t>(1), std::forward<Func>(func), part);
  tfExecutor().run(tf).wait();

#elif defined(TBB_MODE)
  static tbb::task_group_context context(
      tbb::task_group_context::bound,
      tbb::task_group_context::default_traits |
          tbb::task_group_context::concurrent_wait);
#if TBB_MODE == TBB_SIMPLE
  const tbb::simple_partitioner part;
#elif TBB_MODE == TBB_AUTO
  const tbb::auto_partitioner part;
#elif TBB_MODE == TBB_AFFINITY
  // "it is important that the same affinity_partitioner object be passed to
  // loop templates to be optimized for affinity" see
  // https://spec.oneapi.io/versions/latest/elements/oneTBB/source/algorithms/partitioners/affinity_partitioner.html
  static tbb::affinity_partitioner part;
#elif TBB_MODE == TBB_CONST_AFFINITY
  tbb::affinity_partitioner part;
#elif TBB_MODE == TBB_RAPID
  // no partitioner
#else
  static_assert(false, "Wrong TBB_MODE mode");
#endif
  // TODO: grain size?
#if TBB_MODE == TBB_RAPID
  RapidGroup.parallel_ranges(from, to, [&](auto from, auto to, auto part) {
    for (size_t i = from; i != to; ++i) {
      func(i);
    }
  });
#else
  tbb::parallel_for(
      tbb::blocked_range(from, to),
      [&](const tbb::blocked_range<size_t> &range) {
        for (size_t i = range.begin(); i != range.end(); ++i) {
          func(i);
        }
      },
      part, context);
#endif
#elif defined(OMP_MODE)
#pragma omp parallel
#if OMP_MODE == OMP_STATIC
#pragma omp for schedule(static)
#elif OMP_MODE == OMP_RUNTIME
#pragma omp for schedule(runtime)
#elif OMP_MODE == OMP_DYNAMIC_MONOTONIC
// TODO: chunk size?
#pragma omp for schedule(monotonic : dynamic)
#elif OMP_MODE == OMP_DYNAMIC_NONMONOTONIC
#pragma omp for schedule(nonmonotonic : dynamic)
#elif OMP_MODE == OMP_GUIDED_MONOTONIC
#pragma omp for schedule(monotonic : guided)
#elif OMP_MODE == OMP_GUIDED_NONMONOTONIC
#pragma omp for schedule(nonmonotonic : guided)
#else
  static_assert(false, "Wrong OMP_MODE mode");
#endif
  for (size_t i = from; i < to; ++i) {
    func(i);
  }
#elif defined(EIGEN_MODE)
  EigenPartitioner::ParallelFor(from, to, func, grainSize);
#else
  static_assert(false, "Wrong mode");
#endif
}

inline void Warmup(size_t threadsNum) {
  SpinBarrier barrier(threadsNum);
  ParallelFor(0, threadsNum, [&barrier](size_t) {
    barrier.Notify();
    barrier.Wait(); // wait for all threads to start
  });
}

inline void InitParallel(size_t threadsNum) {
#if defined(TBB_MODE) && TBB_MODE == TBB_RAPID
  static InitOnce rapidInit{[threadsNum]() { RapidGroup.init(threadsNum); }};
#endif
#ifdef HPX_MODE
  GetHugePseudoIterator();
  static InitOnce warmup{[threadsNum]() {
    ParallelFor(0, threadsNum * threadsNum, [](size_t) {
      for (size_t i = 0; i != 1000000; ++i) {
        // do nothing
        CpuRelax();
      }
    });
  }};
#endif
#ifdef TBB_MODE
  static PinningObserver pinner; // just init observer
  static tbb::global_control threadLimit(
      tbb::global_control::max_allowed_parallelism, threadsNum);
#endif
#ifdef OMP_MODE
  static InitOnce ompInit{[threadsNum]() { omp_set_num_threads(threadsNum); }};
#endif
#ifdef EIGEN_MODE
#if EIGEN_MODE != EIGEN_RAPID
  static EigenPinner pinner(threadsNum);
#endif
#endif
#if OMP_MODE == OMP_RUNTIME
  // lb4omp doesn't work well with barrier :(
  static InitOnce warmup{[threadsNum]() {
    ParallelFor(0, threadsNum * threadsNum, [](size_t) {
      for (size_t i = 0; i != 1000000; ++i) {
        // do nothing
        CpuRelax();
      }
    });
  }};
#else
  static InitOnce warmup{[threadsNum]() { Warmup(threadsNum); }};
#endif
}
