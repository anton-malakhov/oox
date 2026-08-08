#pragma once
#include "eigen_pool.h"
#include "poor_barrier.h"

#if defined(EIGEN_MODE) && EIGEN_MODE != EIGEN_RAPID

struct EigenPinner {
  EigenPinner(size_t threadsNum) {
    // use ptr because we want to wait for all threads in other threads
    auto barrier = std::make_shared<SpinBarrier>(threadsNum - 1);
    for (size_t i = 1; i < threadsNum; ++i) { // don't pin main thread
      EigenPool().RunOnThread(Eigen::MakeTask([barrier, i]() {
                              PinThread(i);
                              barrier->Notify();
                              barrier->Wait();
                            }),
                            i);
    }
    PinThread(0);
    barrier->Wait();
  }
};

#endif
