#pragma once

#include "modes.h"
#include "poor_barrier.h"
#include "timespan_partitioner.h"
#include "util.h"
#include <vector>
#include <algorithm>
#include <utility>
#include <numeric>

#if TBB_MODE == TBB_RAPID
#include "rapid_start.h"
inline Harness::RapidStart RapidGroup;
#endif

#if EIGEN_MODE == EIGEN_RAPID
#include "rapid_start.h"
inline Harness::RapidStart<EigenPoolWrapper> RapidGroup;
#endif

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

#ifdef EIGEN_MODE
// TODO: move to eigen header
template <typename F>
inline void EigenParallelFor(size_t from, size_t to, F &&func) {
#if EIGEN_MODE == EIGEN_SIMPLE
  EigenPartitioner::ParallelForSimple<EigenPoolWrapper>(from, to,
                                                        std::forward<F>(func));
#elif EIGEN_MODE == EIGEN_TIMESPAN
  EigenPartitioner::ParallelForTimespan<EigenPoolWrapper,
                                        EigenPartitioner::GrainSize::DEFAULT>(
      from, to, std::forward<F>(func));
#elif EIGEN_MODE == EIGEN_TIMESPAN_GRAINSIZE
  EigenPartitioner::ParallelForTimespan<EigenPoolWrapper,
                                        EigenPartitioner::GrainSize::AUTO>(
      from, to, std::forward<F>(func));
#elif EIGEN_MODE == EIGEN_STATIC
  EigenPartitioner::ParallelForStatic<EigenPoolWrapper>(from, to,
                                                        std::forward<F>(func));
#elif EIGEN_MODE == EIGEN_RAPID
  RapidGroup.parallel_ranges(from, to, [&func](auto from, auto to, auto part) {
    for (size_t i = from; i != to; ++i) {
      func(i);
    }
  });
#else
  static_assert(false, "Wrong EIGEN_MODE mode");
#endif
}
#endif

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
  EigenParallelFor(from, to, func);
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
#if TBB_MODE == TBB_RAPID || EIGEN_MODE == EIGEN_RAPID
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
