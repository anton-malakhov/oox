#ifndef PARLAY_INTERNAL_SCHEDULER_PLUGINS_OPENCILK_H_
#define PARLAY_INTERNAL_SCHEDULER_PLUGINS_OPENCILK_H_

#if !defined(PARLAY_OPENCILK)
#error "Undefined opencilk"
#endif

#include <cstddef>

#include <type_traits>
#include "../../parallel.h"

#include <cilk/cilk.h>
#include <cilk/cilk_api.h>

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

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wdeprecated-declarations"

inline size_t worker_id() { return __cilkrts_get_worker_number(); }

#pragma clang diagnostic pop

template <typename Lf, typename Rf>
inline void par_do(Lf&& left, Rf&& right, bool) {
  static_assert(std::is_invocable_v<Lf&&>);
  static_assert(std::is_invocable_v<Rf&&>);
  cilk_spawn std::forward<Rf>(right)();
  std::forward<Lf>(left)();
  cilk_sync;
}

template <typename F>
inline void parallel_for(size_t start, size_t end, F&& f, long granularity, bool) {
  static_assert(std::is_invocable_v<F&, size_t>);
  if (granularity == 0)
    cilk_for (size_t i=start; i<end; i++) f(i);
  else if ((end - start) <= static_cast<size_t>(granularity))
    for (size_t i=start; i < end; i++) f(i);
  else {
    size_t n = end-start;
    size_t mid = (start + (9*(n+1))/16);
    cilk_spawn parallel_for(start, mid, f, granularity);
    parallel_for(mid, end, f, granularity);
    cilk_sync;
  }
}

inline void init_plugin_internal() {
  auto num_threads = num_workers();
}

template <typename... Fs>
void execute_with_scheduler(Fs...) {
  struct Illegal {};
  static_assert((std::is_same_v<Illegal, Fs> && ...), "parlay::execute_with_scheduler is only available in the Parlay scheduler and is not compatible with OpenCilk");
}

}  // namespace parlay

#endif  // PARLAY_INTERNAL_SCHEDULER_PLUGINS_OPENCILK_H_

