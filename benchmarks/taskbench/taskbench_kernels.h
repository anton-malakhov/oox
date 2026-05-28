#ifndef OOX_BENCH_TASKBENCH_KERNELS_H
#define OOX_BENCH_TASKBENCH_KERNELS_H

#include "taskbench_config.h"

#include <algorithm>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <vector>

namespace oox_bench::taskbench {

inline std::uint64_t mix64(std::uint64_t x) {
  x += 0x9e3779b97f4a7c15ULL;
  x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9ULL;
  x = (x ^ (x >> 27)) * 0x94d049bb133111ebULL;
  return x ^ (x >> 31);
}

inline std::uint64_t hash_point(std::uint64_t seed, int graph, int row, int col) {
  std::uint64_t x = seed;
  x ^= mix64(static_cast<std::uint64_t>(graph) + 0x10000ULL);
  x ^= mix64(static_cast<std::uint64_t>(row) + 0x20000ULL);
  x ^= mix64(static_cast<std::uint64_t>(col) + 0x30000ULL);
  return mix64(x);
}

inline double normalized_hash(std::uint64_t seed, int graph, int row, int col) {
  const auto value = hash_point(seed, graph, row, col) >> 11;
  constexpr double denom = static_cast<double>(1ULL << 53);
  return static_cast<double>(value) / denom;
}

inline long local_iterations(const Config& cfg, int graph, int row, int col) {
  if (cfg.iterations == 0) return 0;
  const double factor = 1.0 + cfg.imbalance * normalized_hash(cfg.seed, graph, row, col);
  return std::max<long>(1, static_cast<long>(std::llround(static_cast<double>(cfg.iterations) * factor)));
}

inline std::uint64_t compute_kernel(long iterations, std::uint64_t x) {
  volatile double a = static_cast<double>((x & 1023U) + 1U);
  for (long k = 0; k < iterations; ++k) {
    a = a * 1.0000001 + 3.141592653589793;
  }
  return static_cast<std::uint64_t>(a) ^ mix64(x + static_cast<std::uint64_t>(iterations));
}

inline std::uint64_t memory_kernel(long iterations, std::size_t scratch_bytes, std::uint64_t seed) {
  if (scratch_bytes == 0 || iterations == 0) {
    return mix64(seed);
  }
  thread_local std::vector<std::byte> scratch;
  if (scratch.size() < scratch_bytes) {
    scratch.resize(scratch_bytes);
  }

  std::uint64_t x = seed;
  for (long k = 0; k < iterations; ++k) {
    x = mix64(x + static_cast<std::uint64_t>(k));
    const auto idx = static_cast<std::size_t>(x % scratch_bytes);
    const auto old = std::to_integer<unsigned char>(scratch[idx]);
    const auto next = static_cast<unsigned char>(old ^ static_cast<unsigned char>(x & 0xffU));
    scratch[idx] = std::byte{next};
    x ^= static_cast<std::uint64_t>(old) + idx;
  }
  return mix64(x);
}

inline std::uint64_t run_kernel(const Config& cfg, int graph, int row, int col, std::uint64_t seed) {
  const long iters = local_iterations(cfg, graph, row, col);
  switch (cfg.kernel) {
    case Kernel::Empty: return mix64(seed);
    case Kernel::Compute: return compute_kernel(iters, seed);
    case Kernel::Memory: return memory_kernel(iters, cfg.scratch_bytes, seed);
  }
  return seed;
}

} // namespace oox_bench::taskbench

#endif // OOX_BENCH_TASKBENCH_KERNELS_H
