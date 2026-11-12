// SPDX-License-Identifier: Apache-2.0
#pragma once
#include "parallel_for.h"

namespace oox::detail::eigen_pool::rapid::policy_detail {

template <typename Policy>
concept SupportedPartitioner =
    (std::is_same_v<std::remove_cvref_t<Policy>, AutoPartitioner> ||
     std::is_same_v<std::remove_cvref_t<Policy>, SimplePartitioner> ||
     std::is_same_v<std::remove_cvref_t<Policy>, StaticPartitioner> ||
     std::is_same_v<std::remove_cvref_t<Policy>, AffinityPartitioner>) &&
    requires(Policy &policy, ThreadPool &pool) { policy.MakeState(pool); };

template <bool Enabled, size_t Capacity> struct SeedPlacement {};
template <size_t Capacity> struct SeedPlacement<true, Capacity> {
  std::array<size_t, Capacity> seed_for_slot;
};

// Build a bounded prefix of the selected policy's splitting tree. No tasks,
// callbacks or additional pool-sized states are created during preparation.
template <typename State, size_t Capacity>
class SeedFrontier : private SeedPlacement<State::uses_affinity, Capacity> {
  static_assert(Capacity >= 2 && Capacity <= 64);
  static_assert(std::is_nothrow_copy_constructible_v<State>);
public:
  using Range = partitioner_detail::LoopRange;
  struct Seed { Range range; State state; };

  SeedFrontier(Range range, State state) noexcept {
    seeds_[0].emplace(Seed{range, std::move(state)});
  }
  void SplitTo(size_t target) noexcept {
    assert(target && target <= Capacity);
    while (size_ < target) {
      size_t selected = size_, largest = 0;
      for (size_t i = 0; i < size_; ++i) {
        const auto &seed = *seeds_[i];
        const size_t length = seed.range.end - seed.range.begin;
        if (length > largest && CanSplit(seed)) {
          selected = i;
          largest = length;
        }
      }
      if (selected == size_) break;
      auto &left = *seeds_[selected];
      // Auto's predicate mutates its budget; commit it only on the chosen leaf.
      const bool divisible = left.state.IsDivisible();
      assert(divisible);
      (void)divisible;
      const auto split = left.state.GetSplit();
      Range right(left.range, split);
      seeds_[size_++].emplace(Seed{right, State(left.state, split)});
    }
  }
  void Assign(size_t caller, const unsigned *workers, size_t helpers) noexcept {
    assert(helpers + 1 <= Capacity);
    if constexpr (State::uses_affinity) {
      std::array<bool, Capacity> used{};
      // Match all available preferred workers before assigning unmatched seeds.
      // An unavailable preference remains a hint, never a readiness barrier.
      for (size_t slot = 0; slot <= helpers; ++slot) {
        this->seed_for_slot[slot] = size_;
        const size_t worker = slot ? workers[slot - 1] : caller;
        if (worker == partitioning::no_affinity) continue;
        for (size_t i = 0; i < size_; ++i)
          if (!used[i] && seeds_[i]->state.Hint() == worker) {
            this->seed_for_slot[slot] = i;
            used[i] = true;
            break;
          }
      }
      size_t next = 0;
      for (size_t slot = 0; slot <= helpers; ++slot) {
        if (this->seed_for_slot[slot] != size_) continue;
        while (next < size_ && used[next]) ++next;
        if (next < size_) this->seed_for_slot[slot] = next++;
      }
    }
  }
  size_t Size() const noexcept { return size_; }
  const Seed &At(size_t index) const noexcept { assert(index < size_); return *seeds_[index]; }
  size_t ForSlot(size_t slot) const noexcept {
    if constexpr (State::uses_affinity) return this->seed_for_slot[slot];
    else return std::min(slot, size_);
  }

private:
  static bool CanSplit(const Seed &seed) noexcept {
    if (!seed.range.IsDivisible()) return false;
    if constexpr (requires(const State &state) { state.IsDivisible(); }) {
      return seed.state.IsDivisible();
    } else {
      State probe(seed.state);
      return probe.IsDivisible();
    }
  }
  std::array<std::optional<Seed>, Capacity> seeds_;
  size_t size_ = 1;
};

} // namespace oox::detail::eigen_pool::rapid::policy_detail
