// SPDX-License-Identifier: Apache-2.0
#pragma once

#include <algorithm>
#include <climits>
#include <cstddef>
#include <thread>
#include <vector>
#if defined(__linux__)
#include <cerrno>
#include <sched.h>
#include <fstream>
#include <set>
#include <string>
#endif

namespace oox::detail::eigen_pool {

#if defined(__linux__) && !(defined(OOX_EIGEN_TWIST) && OOX_EIGEN_TWIST)
namespace affinity_detail {
using CpuMask = std::vector<unsigned long>;

inline bool ReadThreadMask(CpuMask &mask) noexcept {
  return sched_getaffinity(0, mask.size() * sizeof(unsigned long),
                           reinterpret_cast<cpu_set_t *>(mask.data())) == 0;
}

inline bool SetThreadMask(const CpuMask &mask) noexcept {
  return sched_setaffinity(0, mask.size() * sizeof(unsigned long),
                           reinterpret_cast<const cpu_set_t *>(mask.data())) == 0;
}

inline bool ContainsCpu(const CpuMask &mask, size_t cpu) noexcept {
  return CPU_ISSET_S(cpu, mask.size() * sizeof(unsigned long),
                     reinterpret_cast<const cpu_set_t *>(mask.data()));
}

inline CpuMask ReadAllowedMask() {
  CpuMask mask(sizeof(cpu_set_t) / sizeof(unsigned long));
  while (!ReadThreadMask(mask)) {
    if (errno != EINVAL) return {};
    // Linux needs room for the kernel mask even when few CPUs are allowed.
    mask.resize(mask.size() * 2);
  }
  return mask;
}

inline std::string ThreadSiblings(size_t cpu) {
  std::ifstream topology("/sys/devices/system/cpu/cpu" + std::to_string(cpu) +
                         "/topology/thread_siblings_list");
  std::string siblings;
  topology >> siblings;
  return siblings;
}
} // namespace affinity_detail
#endif

struct ResidentPlacement {
  size_t limit;
  struct Binding {
#if defined(__linux__) && !(defined(OOX_EIGEN_TWIST) && OOX_EIGEN_TWIST)
    Binding(size_t cpu, size_t words) : target_cpu(cpu), target(words), original(words) {
      CPU_SET_S(cpu, words * sizeof(unsigned long), reinterpret_cast<cpu_set_t *>(target.data()));
    }
    size_t target_cpu;
    affinity_detail::CpuMask target, original;
    bool applied = false;

    void Apply() noexcept {
      if (applied || !affinity_detail::ReadThreadMask(original)) return;
      // The worker may have narrowed its own affinity since pool construction.
      if (affinity_detail::ContainsCpu(original, target_cpu))
        applied = affinity_detail::SetThreadMask(target);
    }
    void Restore() noexcept {
      if (applied && affinity_detail::SetThreadMask(original)) applied = false;
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
  const auto mask = affinity_detail::ReadAllowedMask();
  if (mask.empty()) return {std::min(workers, size_t{1}), {}};
  ResidentPlacement placement{0, {}};
  std::set<std::string> cores;
  const size_t bits = mask.size() * sizeof(unsigned long) * CHAR_BIT;
  for (size_t cpu = 0; cpu < bits && placement.bindings.size() < workers; ++cpu) {
    if (!affinity_detail::ContainsCpu(mask, cpu)) continue;
    const auto siblings = affinity_detail::ThreadSiblings(cpu);
    if (siblings.empty()) {
      const size_t logical = CPU_COUNT_S(mask.size() * sizeof(unsigned long),
                                         reinterpret_cast<const cpu_set_t *>(mask.data()));
      return {std::min(workers, std::max(size_t{1}, logical)), {}};
    }
    if (cores.insert(siblings).second)
      placement.bindings.emplace_back(cpu, mask.size());
  }
  placement.limit = std::min(workers, std::max(size_t{1}, placement.bindings.size()));
  return placement;
#else
  return {std::min(workers, std::max(size_t{1},
      static_cast<size_t>(std::thread::hardware_concurrency()))), {}};
#endif
}

inline size_t AvailableResidentLimit(size_t workers) {
  return DiscoverResidentPlacement(workers).limit;
}

} // namespace oox::detail::eigen_pool
