// SPDX-License-Identifier: Apache-2.0
#pragma once
#include <oox/eigen/parallel_for.h>
#include <cstdlib>
#include <iostream>
#include <string_view>

namespace eigen_partitioner_test {
enum class Policy { Auto, Simple, Static, Affinity };
inline Policy policy = Policy::Auto;
inline oox::detail::eigen_pool::AffinityPartitioner affinity;

inline void Select(int argc, char **argv) {
  const std::string_view name = argc > 1 ? argv[1] : "auto";
  if (name == "auto") policy = Policy::Auto;
  else if (name == "simple") policy = Policy::Simple;
  else if (name == "static") policy = Policy::Static;
  else if (name == "affinity") policy = Policy::Affinity;
  else { std::cerr << "unknown partitioner: " << name << '\n'; std::_Exit(64); }
}
template <typename F>
void Run(oox::detail::eigen_pool::ThreadPool &pool, size_t begin, size_t end,
         F &&function, size_t grain = 1,
         oox::detail::eigen_pool::partitioner_detail::Metrics *metrics = nullptr) {
  using namespace oox::detail::eigen_pool;
  switch (policy) {
  case Policy::Auto:
    return ParallelFor(pool, begin, end, std::forward<F>(function),
                       AutoPartitioner{}, grain, metrics);
  case Policy::Simple:
    return ParallelFor(pool, begin, end, std::forward<F>(function),
                       SimplePartitioner{}, grain, metrics);
  case Policy::Static:
    return ParallelFor(pool, begin, end, std::forward<F>(function),
                       StaticPartitioner{}, grain, metrics);
  case Policy::Affinity:
    return ParallelFor(pool, begin, end, std::forward<F>(function),
                       affinity, grain, metrics);
  }
}
} // namespace eigen_partitioner_test
