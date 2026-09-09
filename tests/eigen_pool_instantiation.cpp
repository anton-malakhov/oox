// SPDX-License-Identifier: Apache-2.0

#include <oox/eigen/nonblocking_thread_pool.h>

template class oox::detail::eigen_pool::ThreadPoolTempl<
    oox::detail::eigen_pool::StlThreadEnvironment>;
#if HAVE_EIGEN_DEMAND
template class oox::detail::eigen_pool::ThreadPoolTempl<
    oox::detail::eigen_pool::StlThreadEnvironment,
    oox::detail::eigen_pool::DemandPolicy>;
#endif

int main() { return 0; }
