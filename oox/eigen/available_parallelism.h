// SPDX-License-Identifier: Apache-2.0
#pragma once

#include <algorithm>
#include <bit>
#include <cstddef>
#include <thread>
#include <vector>
#include <utility>
#if defined(__linux__)
#include <cerrno>
#include <sched.h>
#include <fstream>
#include <set>
#include <string>
#endif

namespace oox::detail::eigen_pool {

struct ResidentPlacement {
  size_t limit;
  struct Binding {
#if defined(__linux__) && !(defined(OOX_EIGEN_TWIST) && OOX_EIGEN_TWIST)
    std::vector<unsigned long> target, original;
    bool applied = false;

    void Apply() noexcept {
      if (applied) return;
      const size_t bytes = original.size() * sizeof(unsigned long);
      if (sched_getaffinity(0, bytes, reinterpret_cast<cpu_set_t *>(original.data())) != 0)
        return;
      for (size_t i = 0; i < target.size(); ++i)
        if (target[i] & ~original[i]) return;
      applied = sched_setaffinity(0, bytes, reinterpret_cast<const cpu_set_t *>(target.data())) == 0;
    }
    void Restore() noexcept {
      if (applied)
        applied = sched_setaffinity(0, original.size() * sizeof(unsigned long),
                      reinterpret_cast<const cpu_set_t *>(original.data())) != 0;
    }
#else
    void Apply() noexcept {}
    void Restore() noexcept {}
#endif
  };
  std::vector<Binding> bindings;
};

inline ResidentPlacement DiscoverResidentPlacement(size_t workers) {
#if defined(OOX_EIGEN_TWIST) && OOX_EIGEN_TWIST
  return {workers, {}};
#elif defined(__linux__)
  std::vector<unsigned long> mask(sizeof(cpu_set_t) / sizeof(unsigned long));
  for (;;) {
    if (sched_getaffinity(0, mask.size() * sizeof(unsigned long),
                         reinterpret_cast<cpu_set_t *>(mask.data())) == 0) {
      size_t count = 0;
      for (auto word : mask) count += std::popcount(word);
      ResidentPlacement placement{std::min(workers, std::max(size_t{1}, count)), {}};
      std::set<std::string> cores;
      for (size_t word = 0; word < mask.size(); ++word) {
        for (auto bits = mask[word]; bits; bits &= bits - 1) {
          const unsigned bit = std::countr_zero(bits);
          const size_t cpu = word * sizeof(unsigned long) * 8 + bit;
          std::ifstream topology("/sys/devices/system/cpu/cpu" + std::to_string(cpu) +
                                 "/topology/thread_siblings_list");
          std::string siblings;
          if (!(topology >> siblings)) return {placement.limit, {}};
          if (!cores.insert(siblings).second) continue;
          ResidentPlacement::Binding binding;
          binding.target.resize(mask.size());
          binding.original.resize(mask.size());
          binding.target[word] = 1ul << bit;
          placement.bindings.push_back(std::move(binding));
          if (placement.bindings.size() == workers) return placement;
        }
      }
      if (!placement.bindings.empty()) placement.limit = placement.bindings.size();
      return placement;
    }
    if (errno != EINVAL) return {std::min(workers, size_t{1}), {}};
    // Linux requires room for its full CPU mask, including unavailable CPUs.
    mask.resize(mask.size() * 2);
  }
#else
  return {std::min(workers, std::max(size_t{1},
      static_cast<size_t>(std::thread::hardware_concurrency()))), {}};
#endif
}

inline size_t AvailableResidentLimit(size_t workers) {
  return DiscoverResidentPlacement(workers).limit;
}

} // namespace oox::detail::eigen_pool
