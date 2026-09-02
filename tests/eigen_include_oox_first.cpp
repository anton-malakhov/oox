// SPDX-License-Identifier: Apache-2.0

#define EIGEN_USE_THREADS
#include <oox/eigen/nonblocking_thread_pool.h>
#include <unsupported/Eigen/CXX11/ThreadPool>

int main() {
  oox::detail::eigen_pool::ThreadPool oox_pool(1);
  Eigen::ThreadPool eigen_pool(1);
  return oox_pool.NumThreads() == 1 && eigen_pool.NumThreads() == 1 ? 0 : 1;
}
