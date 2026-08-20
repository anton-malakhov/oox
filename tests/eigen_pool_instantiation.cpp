// SPDX-License-Identifier: Apache-2.0

#include <oox/eigen/nonblocking_thread_pool.h>

template class oox::detail::eigen_pool::ThreadPoolTempl<
    oox::detail::eigen_pool::StlThreadEnvironment>;

int main() { return 0; }
