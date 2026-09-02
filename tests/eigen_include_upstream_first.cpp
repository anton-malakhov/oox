// SPDX-License-Identifier: Apache-2.0

#define EIGEN_USE_THREADS
#include <unsupported/Eigen/CXX11/ThreadPool>
#include <oox/eigen/nonblocking_thread_pool.h>

int main() {
  Eigen::ThreadPool eigen_pool(1);
  oox::detail::eigen_pool::ThreadPool oox_pool(1);
  return eigen_pool.NumThreads() == 1 && oox_pool.NumThreads() == 1 ? 0 : 1;
}
