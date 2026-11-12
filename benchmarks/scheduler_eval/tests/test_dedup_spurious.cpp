// SPDX-License-Identifier: Apache-2.0

#include <atomic>
#include <cstdint>
#include <iostream>
#include <limits>
#include <random>
#include <vector>

namespace {

bool inject_failure = false;
bool injected = false;

bool DedupTestCas(std::atomic<std::uint64_t> &slot, std::uint64_t &expected,
                  std::uint64_t desired) {
  if (inject_failure && slot.load() == expected) {
    inject_failure = false;
    injected = true;
    return false;
  }
  return slot.compare_exchange_strong(expected, desired);
}

} // namespace

// Compile the real algorithm with one deterministic weak-CAS spurious failure.
#define SERIAL 1
#define OOX_EVAL_TEST_DEDUP_CAS DedupTestCas
#include "../workloads/primary_workloads.cpp"
#undef OOX_EVAL_TEST_DEDUP_CAS

namespace {

bool Check(const std::vector<std::uint32_t> &keys, std::uint64_t seed,
           std::size_t case_index) {
  auto expected = keys;
  std::sort(expected.begin(), expected.end());
  expected.erase(std::unique(expected.begin(), expected.end()), expected.end());
  for (bool fault : {false, true}) {
    inject_failure = fault;
    injected = false;
    const auto actual = scheduler_eval::RemoveDuplicatesParallel(keys);
    if (actual != expected || injected != (fault && !keys.empty())) {
      std::cerr << "dedup seed=" << seed << " case=" << case_index
                << " size=" << keys.size() << " inject=" << fault
                << " injected=" << injected << " expected_size=" << expected.size()
                << " actual_size=" << actual.size() << '\n';
      return false;
    }
  }
  return true;
}

} // namespace

int main() {
  constexpr std::uint64_t seed = 0x5eed;
  std::mt19937_64 random(seed);
  std::size_t case_index = 0;
  if (!Check({7, 7}, seed, case_index++)) return 1;
  for (std::size_t size = 0; size <= 64; ++size) {
    for (unsigned pattern = 0; pattern < 5; ++pattern) {
      std::vector<std::uint32_t> keys(size);
      for (std::size_t i = 0; i < size; ++i) {
        switch (pattern) {
        case 0: keys[i] = 0; break;
        case 1: keys[i] = std::numeric_limits<std::uint32_t>::max(); break;
        case 2: keys[i] = static_cast<std::uint32_t>(i * 1024); break;
        case 3: keys[i] = static_cast<std::uint32_t>(random() % 7); break;
        case 4: keys[i] = static_cast<std::uint32_t>(random()); break;
        }
      }
      if (!Check(keys, seed, case_index++)) return 1;
    }
  }
  for (auto size : {65537u, 262145u, 1048577u}) {
    std::vector<std::uint32_t> keys(size);
    for (auto &key : keys) key = static_cast<std::uint32_t>(random() % (size / 3));
    if (!Check(keys, seed, case_index++)) return 1;
  }
  std::cout << "dedup oracle: " << case_index
            << " cases passed, each with and without injected spurious failure\n";
}
