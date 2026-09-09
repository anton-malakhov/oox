// SPDX-License-Identifier: Apache-2.0

#include <oox/oox.h>
#include <gtest/gtest.h>

#include <atomic>
#include <cstdint>
#include <memory>
#include <random>
#include <vector>

namespace {

std::uint64_t Combine(std::uint64_t a, std::uint64_t b,
                      std::uint64_t index) noexcept {
  return (a * 0x9e3779b97f4a7c15ULL) ^ (b + index);
}

// The oracle is a serial topological evaluation of explicit parent indices,
// independent of OOX dependency construction and the scheduler partitioner.
void CheckDag(std::size_t count, std::uint64_t seed, unsigned shape) {
  SCOPED_TRACE(::testing::Message() << "seed=" << seed << " count=" << count
                                   << " shape=" << shape);
  std::mt19937_64 random(seed);
  std::vector<std::uint64_t> expected(count + 1, seed);
  std::vector<oox::var<std::uint64_t>> nodes;
  nodes.reserve(count + 1);
  nodes.emplace_back(seed);
  auto visits = std::make_unique<std::atomic<unsigned>[]>(count + 1);
  for (std::size_t i = 1; i <= count; ++i) {
    const std::size_t a = shape == 0 ? i - 1
        : shape == 1 ? 0 : random() % i;
    const std::size_t b = shape == 2 ? i / 2 : random() % i;
    expected[i] = Combine(expected[a], expected[b], i);
    nodes.emplace_back(oox::run([&, i](std::uint64_t x, std::uint64_t y) noexcept {
      visits[i].fetch_add(1, std::memory_order_relaxed);
      return Combine(x, y, i);
    }, nodes[a], nodes[b]));
  }
  for (std::size_t i = 1; i <= count; ++i) {
    ASSERT_EQ(oox::wait_and_get(nodes[i]), expected[i]) << "vertex=" << i;
    ASSERT_EQ(visits[i].load(), 1u) << "vertex=" << i;
  }
}

TEST(EigenBackendDag, DenseSmallGeneratedCases) {
  for (std::size_t count = 0; count <= 129; ++count)
    for (unsigned shape = 0; shape < 4; ++shape)
      CheckDag(count, 0x4f4f58544242ULL + count * 4 + shape, shape);
}

TEST(EigenBackendDag, LargeGeneratedCases) {
  for (unsigned shape = 0; shape < 4; ++shape)
    CheckDag(65536, 0x4c41524745544242ULL + shape, shape);
}

} // namespace
