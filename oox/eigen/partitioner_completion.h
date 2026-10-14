// Copyright (c) 2005-2021 Intel Corporation
// SPDX-License-Identifier: Apache-2.0
#pragma once
#include "partitioner_region.h"

namespace oox::detail::eigen_pool::partitioner_detail {

// The two subtree references and the task-execution bit share one counter.
// The embedded node can outlive execution, or finish before execution returns.
template <typename Owner, bool Enabled = true> struct PeerJoin {
  explicit PeerJoin(PeerJoin *parent) noexcept : parent(parent) {}
  static constexpr uintptr_t root_bit = 1;
  static PeerJoin *Root(Region &region) noexcept {
    static_assert(alignof(Region) > root_bit);
    // The final branch releases the tree's single Region reference.
    return reinterpret_cast<PeerJoin *>(
        reinterpret_cast<uintptr_t>(&region) | root_bit);
  }
  static void CompleteRoot(PeerJoin *node, bool notify) noexcept {
    const auto address = reinterpret_cast<uintptr_t>(node) & ~root_bit;
    reinterpret_cast<Region *>(address)->TaskComplete(notify);
  }
  static bool IsRoot(PeerJoin *node) noexcept {
    return reinterpret_cast<uintptr_t>(node) & root_bit;
  }
  static bool PeerStolen(PeerJoin *node) noexcept {
    return node && !IsRoot(node) && node->stolen.load(std::memory_order_relaxed);
  }
  bool HasPeer() const noexcept {
    return (references.load(std::memory_order_acquire) & branch_mask) == 2;
  }
  static void Release(PeerJoin *node) noexcept {
    if (node && IsRoot(node)) {
      // The root Process returns this branch before its caller starts waiting.
      CompleteRoot(node, false);
      return;
    }
    while (node) {
      // Capture metadata before the RMW can allow FinishExecution to free us.
      PeerJoin *parent = node->parent;
      std::thread::id caller;
      if constexpr (requires(const Owner &owner) { owner.PublishingThreadId(); }) {
        if (IsRoot(parent))
          caller = static_cast<const Owner *>(node)->PublishingThreadId();
      }
      const unsigned previous =
          node->references.fetch_sub(1, std::memory_order_acq_rel);
      if ((previous & branch_mask) != 1)
        return;
      if (!(previous & executing))
        DeleteSmallObject(static_cast<Owner *>(node));
      if (IsRoot(parent)) {
        // The first task was published by the synchronous caller. That thread
        // cannot be parked while executing this completion; other threads notify.
        CompleteRoot(parent, caller == std::thread::id{} ||
                             caller != std::this_thread::get_id());
        return;
      }
      node = parent;
    }
  }
  void FinishExecution() noexcept {
    if (references.fetch_sub(executing, std::memory_order_acq_rel) == executing)
      DeleteSmallObject(static_cast<Owner *>(this));
  }

  static constexpr unsigned branch_mask = 3;
  static constexpr unsigned executing = 4;
  PeerJoin *const parent;
  std::atomic<unsigned> references{executing | 2};
  std::atomic<bool> stolen{false};
};

// Non-adaptive policies already join through Region's task count.
template <typename Owner> struct PeerJoin<Owner, false> {
  explicit constexpr PeerJoin(PeerJoin *) noexcept {}
  static constexpr void Release(PeerJoin *) noexcept {}
  void FinishExecution() noexcept { DeleteSmallObject(static_cast<Owner *>(this)); }
};

template <typename Owner, bool Enabled> struct Branch {
  using Join = PeerJoin<Owner, Enabled>;
  explicit Branch(Join *parent) noexcept : parent_(parent) {}
  ~Branch() { Join::Release(parent_); }
  Join *Parent() const noexcept { return parent_; }
  void SetParent(Join *parent) noexcept { parent_ = parent; }

private:
  Join *parent_;
};

template <typename Owner> struct Branch<Owner, false> {
  using Join = PeerJoin<Owner, false>;
  explicit constexpr Branch(Join *) noexcept {}
  constexpr Join *Parent() const noexcept { return nullptr; }
  constexpr void SetParent(Join *) noexcept {}
};

} // namespace oox::detail::eigen_pool::partitioner_detail
