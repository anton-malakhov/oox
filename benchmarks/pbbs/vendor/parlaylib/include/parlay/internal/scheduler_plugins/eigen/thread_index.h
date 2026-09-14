#pragma once

#include "eigen_pool.h"

using ThreadId = size_t;

inline ThreadId GetThreadIndex() {
  thread_local static size_t id = [] {
    return EigenPool().CurrentThreadId();
  }();
  return id;
}
