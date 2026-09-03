#pragma once
#include "modes.h"
#include "num_threads.h"

#ifdef EIGEN_MODE

#define EIGEN_USE_THREADS
#include "oox/eigen/nonblocking_thread_pool.h"

inline oox::detail::eigen_pool::ThreadPool& EigenPool() {
  static auto pool =
      oox::detail::eigen_pool::ThreadPool(GetNumThreads(), true, true);
  return pool;
}

class EigenPoolWrapper {
public:
  template <typename F> void run(F &&f) {
    EigenPool().Schedule(
        oox::detail::eigen_pool::MakeTask(std::forward<F>(f)));
  }

  template <typename F> void run_on_thread(F &&f, size_t hint) {
    EigenPool().RunOnThread(
        oox::detail::eigen_pool::MakeTask(std::forward<F>(f)), hint);
  }

  bool join_main_thread() { return EigenPool().JoinMainThread(); }

  bool execute_something_else() {
    return EigenPool().TryExecuteSomething();
  }
};

#endif
