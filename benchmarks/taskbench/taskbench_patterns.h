#ifndef OOX_BENCH_TASKBENCH_PATTERNS_H
#define OOX_BENCH_TASKBENCH_PATTERNS_H

#include "taskbench_config.h"
#include "taskbench_kernels.h"

#include <algorithm>
#include <vector>

#ifndef OOX_TASKBENCH_MAX_DEPS
#define OOX_TASKBENCH_MAX_DEPS 8
#endif

namespace oox_bench::taskbench {

inline void append_dep_if_valid(std::vector<int>& out, int width, int col) {
  if (col >= 0 && col < width) out.push_back(col);
}

inline void nearest_deps(std::vector<int>& out, const Config& cfg, int width, int col) {
  if (cfg.radix == 0) return;
  const int left = cfg.radix / 2;
  for (int k = 0; k < cfg.radix; ++k) {
    append_dep_if_valid(out, width, col - left + k);
  }
}

inline void spread_deps(std::vector<int>& out, const Config& cfg, int width, int col) {
  if (cfg.radix == 0) return;
  if (cfg.radix == 1 || width == 1) {
    out.push_back(col);
    return;
  }
  for (int k = 0; k < cfg.radix; ++k) {
    const auto offset = static_cast<long long>(k) * std::max(1, width - 1) / std::max(1, cfg.radix - 1);
    append_dep_if_valid(out, width, static_cast<int>((col + offset) % width));
  }
}

inline void fft_deps(std::vector<int>& out, int width, int row, int col) {
  out.push_back(col);
  int levels = 0;
  for (int w = width; w > 1; w >>= 1) ++levels;
  const int bit = (row - 1) % std::max(1, levels);
  out.push_back(col ^ (1 << bit));
}

inline void tree_deps(std::vector<int>& out, int width, int row, int col) {
  out.push_back(col);
  int levels = 0;
  for (int w = width; w > 1; w >>= 1) ++levels;
  if (levels == 0) return;
  const int phase = (row - 1) % (2 * levels);
  const int level = phase < levels ? phase : (2 * levels - phase - 1);
  out.push_back(col ^ (1 << level));
}

inline void random_deps(std::vector<int>& out, const Config& cfg, int width, int row, int col) {
  const int count = std::min(cfg.radix, width);
  for (int candidate = 0; candidate < width && static_cast<int>(out.size()) < count; ++candidate) {
    const auto h = mix64(cfg.seed ^ hash_point(static_cast<std::uint64_t>(row), col, candidate, width));
    if (static_cast<int>(h % width) < count) {
      out.push_back(candidate);
    }
  }
  for (int salt = 0; static_cast<int>(out.size()) < count; ++salt) {
    out.push_back(static_cast<int>(mix64(cfg.seed + hash_point(row, col, salt, count)) % width));
    std::sort(out.begin(), out.end());
    out.erase(std::unique(out.begin(), out.end()), out.end());
  }
}

inline std::vector<int> dependency_columns(const Config& cfg, int width, int row, int col) {
  if (row == 0 || cfg.pattern == Pattern::Trivial) return {};

  std::vector<int> out;
  switch (cfg.pattern) {
    case Pattern::Trivial:
      break;
    case Pattern::Stencil:
      append_dep_if_valid(out, width, col - 1);
      append_dep_if_valid(out, width, col);
      append_dep_if_valid(out, width, col + 1);
      break;
    case Pattern::Sweep:
      append_dep_if_valid(out, width, col - 1);
      append_dep_if_valid(out, width, col);
      break;
    case Pattern::Nearest:
      nearest_deps(out, cfg, width, col);
      break;
    case Pattern::Spread:
      spread_deps(out, cfg, width, col);
      break;
    case Pattern::FFT:
      fft_deps(out, width, row, col);
      break;
    case Pattern::Tree:
      tree_deps(out, width, row, col);
      break;
    case Pattern::Random:
      random_deps(out, cfg, width, row, col);
      break;
  }

  std::sort(out.begin(), out.end());
  out.erase(std::unique(out.begin(), out.end()), out.end());
  if (out.size() > OOX_TASKBENCH_MAX_DEPS) {
    out.resize(OOX_TASKBENCH_MAX_DEPS);
  }
  return out;
}

} // namespace oox_bench::taskbench

#endif // OOX_BENCH_TASKBENCH_PATTERNS_H
