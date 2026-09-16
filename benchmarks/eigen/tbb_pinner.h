#pragma once

#include "util.h"
#include <iostream>
#if defined(__linux__)
#include <sched.h>
#endif
#include <tbb/tbb.h>

class PinningObserver : public tbb::task_scheduler_observer {
public:
  PinningObserver() { observe(true); }

  void on_scheduler_entry(bool is_worker) {
    PinThread(tbb::this_task_arena::current_thread_index());
  }

  ~PinningObserver() { observe(false); }
};
