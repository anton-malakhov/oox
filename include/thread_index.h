#pragma once

#ifdef EIGEN_MODE
#include "eigen_pool.h"
#endif

using ThreadId = int;

inline ThreadId GetThreadIndex() {
  thread_local static int id = [] {
#if defined(TBB_MODE)
    return tbb::this_task_arena::current_thread_index();
#elif defined(OMP_MODE)
    return omp_get_thread_num();
#elif defined(SERIAL)
    return 0;
#elif defined(EIGEN_MODE)
    return EigenPool().CurrentThreadId();
#elif defined(TASKFLOW_MODE)
    return tfExecutor().this_worker_id();
#else
#error "Unsupported mode"
#endif
  }();
  return id;
}