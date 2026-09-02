// This file is part of Eigen, a lightweight C++ template library
// for linear algebra.
//
// Copyright (C) 2014 Benoit Steiner <benoit.steiner.goog@gmail.com>
//
// This Source Code Form is subject to the terms of the Mozilla
// Public License v. 2.0. If a copy of the MPL was not distributed
// with this file, You can obtain one at http://mozilla.org/MPL/2.0/.

#ifndef OOX_EIGEN_STL_THREAD_ENV_H
#define OOX_EIGEN_STL_THREAD_ENV_H

#include <functional>
#include <thread>
#include <utility>

namespace oox::detail::eigen_pool {

struct StlThreadEnvironment {
  // EnvThread constructor must start the thread,
  // destructor must join the thread.
  class EnvThread {
  public:
    template <typename F> EnvThread(F &&f) : thr_(std::forward<F>(f)) {}
    ~EnvThread() { thr_.join(); }
    // This function is called when the threadpool is cancelled.
    void OnCancel() {}

  private:
    std::thread thr_;
  };

  EnvThread *CreateThread(std::function<void()> f) {
    return new EnvThread(std::move(f));
  }
};

} // namespace oox::detail::eigen_pool

#endif // OOX_EIGEN_STL_THREAD_ENV_H
