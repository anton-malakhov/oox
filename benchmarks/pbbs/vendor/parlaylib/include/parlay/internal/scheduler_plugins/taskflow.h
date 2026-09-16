#ifndef PARLAY_INTERNAL_SCHEDULER_PLUGINS_TASKFLOW_H_
#define PARLAY_INTERNAL_SCHEDULER_PLUGINS_TASKFLOW_H_

#if !defined(PARLAY_TASKFLOW) || !defined(TASKFLOW_MODE)
#error "Undefined taskflow"
#endif

#include "taskflow/taskflow/taskflow.hpp"
#include "taskflow/taskflow/algorithm/for_each.hpp"
#include "eigen/modes.h"

namespace parlay {

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

inline tf::Executor& tfExecutor() {
    static tf::Executor exec(num_workers());
    return exec;
}

inline size_t worker_id() {
    return tfExecutor().this_worker_id();
}

template <typename F>
inline void parallel_for(size_t start, size_t end, F&& f, long granularity, bool) {
    tf::Taskflow tf;

#if TASKFLOW_MODE == TASKFLOW_GUIDED
    tf::GuidedPartitioner execution_policy(granularity);
#elif TASKFLOW_MODE == TASKFLOW_DYNAMIC
    tf::DynamicPartitioner execution_policy(granularity);
#elif TASKFLOW_MODE == TASKFLOW_STATIC
    tf::StaticPartitioner execution_policy(granularity);
#elif TASKFLOW_MODE == TASKFLOW_RANDOM
    tf::RandomPartitioner execution_policy(granularity);
#else
    static_assert(false, "Wrong TASKFLOW_MODE mode");
#endif  // TASKFLOW_MODE

    tf.for_each_index(start, end, static_cast<size_t>(1), std::forward<F>(f), execution_policy);

    tfExecutor().run(tf).get();
}

template <typename Lf, typename Rf>
inline void par_do(Lf&& left, Rf&& right, bool) {
    tf::Taskflow tf;

    tf.emplace(std::forward<Lf>(left));
    auto fut = tfExecutor().run(tf);

    std::forward<Rf>(right)();

    fut.get();
}

inline void init_plugin_internal() {
    auto num_threads = num_workers();
    auto& exec = tfExecutor();
}

template <typename... Fs>
void execute_with_scheduler(Fs...) {
    struct Illegal {};
    static_assert((std::is_same_v<Illegal, Fs> && ...), "parlay::execute_with_scheduler is only available in the Parlay scheduler and is not compatible with Taskflow");
}

}  // namespace parlay

#endif  // PARLAY_INTERNAL_SCHEDULER_PLUGINS_TASKFLOW_H_
