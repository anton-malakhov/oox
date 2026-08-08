#pragma once
#include <string>

#define STR_(x) #x
#define STR(x) STR_(x)

inline std::string GetParallelMode() {
#if defined(SERIAL)
  return "SERIAL";
#elif defined(TBB_MODE)
  return STR(TBB_MODE);
#elif defined(OMP_MODE)
  return STR(OMP_MODE);
#elif defined(EIGEN_MODE)
  return STR(EIGEN_MODE);
#elif defined(TASKFLOW_MODE)
  return STR(TASKFLOW_MODE);
#elif defined(HAVE_EIGEN)
  return "EIGEN";
#else
  return "UNKNOWN";
#endif
}

#define OMP_STATIC 1
#define OMP_DYNAMIC_MONOTONIC 2
#define OMP_DYNAMIC_NONMONOTONIC 3
#define OMP_GUIDED_MONOTONIC 4
#define OMP_GUIDED_NONMONOTONIC 5
#define OMP_RUNTIME 6

#define TBB_SIMPLE 1
#define TBB_AUTO 2
#define TBB_AFFINITY 3
#define TBB_CONST_AFFINITY 4
#define TBB_RAPID 5

#define EIGEN_STEALING 1
#define EIGEN_SHARING 2
#define EIGEN_STEALING_GRAINSIZE 3
#define EIGEN_SHARING_STEALING 4

#define TASKFLOW_GUIDED 1
#define TASKFLOW_DYNAMIC 2

#ifdef TBB_MODE
#include <tbb/parallel_for.h>
#endif

#ifdef TASKFLOW_MODE
#include <taskflow/taskflow.hpp>
#include <taskflow/algorithm/for_each.hpp>
#endif

#ifdef OMP_MODE
#include <omp.h>
#endif
