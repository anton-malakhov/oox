#ifndef PARLAY_INTERNAL_SCHEDULER_PLUGINS_TBB_H_
#define PARLAY_INTERNAL_SCHEDULER_PLUGINS_TBB_H_

#if !defined(PARLAY_TBB) || !defined(TBB_MODE)
#error "Undefined tbb"
#endif

#include <cstddef>

#include <type_traits>

#include <tbb/blocked_range.h>
#include <tbb/parallel_for.h>
#include <tbb/parallel_invoke.h>
#include <tbb/partitioner.h>
#include <tbb/task_arena.h>

#include "common/tbb_pinner.h"
#include "eigen/modes.h"

namespace parlay {

// IWYU pragma: private, include "../../parallel.h"

inline size_t num_workers() {
  // cache result to avoid calling getenv on every call
  static size_t threads = []() -> size_t {
    if (const char *envThreads = std::getenv("BENCH_NUM_THREADS")) {
      return std::stoul(envThreads);
    }
    // left just for compatibility
    if (const char *envThreads = std::getenv("OMP_NUM_THREADS")) {
      return std::stoul(envThreads);
    }
    // left just for compatibility
    if (const char *envThreads = std::getenv("CILK_NWORKERS")) {
      return std::stoul(envThreads);
    }
    return std::thread::hardware_concurrency();
  }();
  return threads;
}

inline size_t worker_id() {
  auto id = tbb::this_task_arena::current_thread_index();
  return id == tbb::task_arena::not_initialized ? 0 : id;
}

template <typename F>
inline void parallel_for(size_t start, size_t end, F&& f, long granularity, bool) {
  static_assert(std::is_invocable_v<F&, size_t>);
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
  #else
    static_assert(false, "Wrong TBB_MODE mode");
  #endif
    // TODO: grain size?

  tbb::parallel_for(
      tbb::blocked_range(start, end, std::max(granularity, long{1})),
      [&](const tbb::blocked_range<size_t> &range) {
        for (size_t i = range.begin(); i != range.end(); ++i) {
          f(i);
        }
      },
      part, context);

}

template <typename Lf, typename Rf>
inline void par_do(Lf&& left, Rf&& right, bool) {
  static_assert(std::is_invocable_v<Lf&&>);
  static_assert(std::is_invocable_v<Rf&&>);
  tbb::parallel_invoke(std::forward<Lf>(left), std::forward<Rf>(right));
}

inline void init_plugin_internal() {
  static PinningObserver pinner; // just init observer
  static tbb::global_control threadLimit(
      tbb::global_control::max_allowed_parallelism, num_workers());
}

template <typename... Fs>
void execute_with_scheduler(Fs...) {
  struct Illegal {};
  static_assert((std::is_same_v<Illegal, Fs> && ...), "parlay::execute_with_scheduler is only available in the Parlay scheduler and is not compatible with TBB");
}

}  // namespace parlay

#endif  // PARLAY_INTERNAL_SCHEDULER_PLUGINS_TBB_H_

