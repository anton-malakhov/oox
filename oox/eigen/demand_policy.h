// SPDX-License-Identifier: Apache-2.0
// TBB-derived partition decisions are in tbb_partitioning.h. This file adapts
// those decisions to groups of already-published Eigen tasks.

#ifndef OOX_EIGEN_DEMAND_POLICY_H
#define OOX_EIGEN_DEMAND_POLICY_H

#include "tbb_partitioning.h"

#include <atomic>
#include <memory>
#include <mutex>
#include <utility>

namespace oox::detail::eigen_pool {

struct DemandStatistics {
  std::size_t groups = 0;
  std::size_t offers = 0;
  std::size_t remote_groups = 0;
  std::size_t feedback = 0;
  std::size_t grouped_tasks = 0;
};

struct SingleTaskPolicy {
  static constexpr bool enabled = false;
};

// Initial distribution is per collected group (one producer), not per global
// algorithm range. Depth one is an adapter setting; the ported TBB default is
// still five. A fixed-depth policy is available for comparison.
template <std::size_t GroupSize = 64, unsigned Depth = 1, bool Feedback = true>
struct TaskGroupPolicy {
  static_assert(GroupSize > 1 && GroupSize <= 1024);
  static_assert(Depth <= partitioning::AutoPartition::max_depth);
  static constexpr bool enabled = true;
  static constexpr std::size_t group_size = GroupSize;
  static constexpr unsigned depth = Depth;
  static constexpr bool feedback = Feedback;
};

using DemandPolicy = TaskGroupPolicy<>;

template <typename Work, typename Policy, bool = Policy::enabled>
class DemandRegistry {};

// No registry storage or acquisition code in the legacy instantiation.
template <typename Work, typename Policy>
class DemandRegistry<Work, Policy, true> {
  using Range = partitioning::IndexRange;
  using RangePool = partitioning::RangePool<Range>;
  using Partition = partitioning::AutoPartition;
  using Split = partitioning::Split;

  struct Branch;
  struct Siblings {
    explicit Siblings(std::shared_ptr<Branch> old)
        : ancestor(std::move(old)) {}
    // Keeping the previous branch alive mirrors TBB's parent join tree.
    std::shared_ptr<Branch> ancestor;
    std::atomic<unsigned> live{0};
    std::atomic<bool> stolen{false};
  };

  struct Branch {
    explicit Branch(std::shared_ptr<Siblings> group) : parent(std::move(group)) {
      parent->live.fetch_add(1, std::memory_order_relaxed);
    }
    ~Branch() { parent->live.fetch_sub(1, std::memory_order_relaxed); }
    std::shared_ptr<Siblings> parent;
  };

  struct Storage {
    std::array<Work, Policy::group_size> tasks{};
  };

  struct Group {
    Group(std::shared_ptr<Storage> data, Range work, Partition part,
          int origin) noexcept
        : storage(std::move(data)), range(work), partition(part),
          origin(origin) {}
    std::shared_ptr<Storage> storage;
    Range range;
    Partition partition;
    std::optional<RangePool> ranges;
    std::shared_ptr<Branch> branch;
    int origin;
    bool started = false;
    Group *previous = nullptr;
    std::unique_ptr<Group> next;
  };

public:
  // Keeps sibling liveness valid through a callback, including nested helping.
  // The task itself may delete itself before this object is destroyed.
  struct Acquired {
    Work task{};
    std::shared_ptr<Branch> branch;
    bool published_group = false;
    explicit operator bool() const noexcept { return task != Work{}; }
  };

  DemandRegistry() = default;
  DemandRegistry(const DemandRegistry &) = delete;
  DemandRegistry &operator=(const DemandRegistry &) = delete;
  ~DemandRegistry() { assert(!head_); }

  // pop is an owner-safe dequeue operation, never a user callback.
  template <typename Pop>
  Acquired AcquireLocal(int worker, Pop pop) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (head_) {
      // Fresh local children stay available during nested helping.
      if (Work task = pop())
        return {task, {}, false};
      return TakeOne(worker, false);
    }
    {
      Work first = pop();
      if (first == Work{})
        return {};
      // If optional allocation fails, return the already-owned first task.
      // No additional task is extracted until all root metadata exists.
      std::unique_ptr<Group> group;
      try {
        auto data = std::make_shared<Storage>();
        group = std::make_unique<Group>(std::move(data), Range{0, 0},
                                       Partition{1, Policy::depth}, worker);
      } catch (const std::bad_alloc &) {
        return {first, {}, false};
      }
      group->storage->tasks[0] = first;
      std::size_t count = 1;
      while (count < Policy::group_size) {
        Work task = pop();
        if (task == Work{})
          break;
        group->storage->tasks[count++] = task;
      }
      if (!count)
        return {};
      if (count == 1)
        return {group->storage->tasks[0], {}, false};
      group->range.end = count;
      Append(std::move(group));
      groups_.fetch_add(1, std::memory_order_relaxed);
      grouped_tasks_.fetch_add(count, std::memory_order_relaxed);
    }
    return TakeOne(worker, true);
  }

  // Called after local acquisition failed; concurrent helpers may nevertheless
  // have installed work in this registry. Append is safe in that case too.
  Acquired AcquireRemote(DemandRegistry &victim, int worker) {
    if (&victim == this)
      return {};
    std::unique_ptr<Group> group;
    {
      std::lock_guard<std::mutex> lock(victim.mutex_);
      if (!victim.head_)
        return {};
      group = victim.Remove(victim.head_.get());
    }
    std::lock_guard<std::mutex> lock(mutex_);
    // A ready remainder may migrate after earlier callbacks started. Preserve
    // its initialization state when resuming the same logical group.
    Append(std::move(group));
    remote_groups_.fetch_add(1, std::memory_order_relaxed);
    return TakeOne(worker, true);
  }

  // Only after workers have joined. Destruction of task objects is kept out of
  // the registry lock, as a captured object can itself interact with a pool.
  template <typename Dispose> void Drain(Dispose dispose) {
    while (head_) {
      auto group = Remove(head_.get());
      if (group->ranges) {
        while (!group->ranges->Empty()) {
          Range work = group->ranges->Back();
          for (std::size_t i = work.begin; i < work.end; ++i)
            dispose(group->storage->tasks[i]);
          group->ranges->PopBack();
        }
      } else {
        for (std::size_t i = group->range.begin; i < group->range.end; ++i)
          dispose(group->storage->tasks[i]);
      }
    }
  }

  DemandStatistics Statistics() const noexcept {
    return {groups_.load(std::memory_order_relaxed),
            offers_.load(std::memory_order_relaxed),
            remote_groups_.load(std::memory_order_relaxed),
            feedback_.load(std::memory_order_relaxed),
            grouped_tasks_.load(std::memory_order_relaxed)};
  }

private:
  void Append(std::unique_ptr<Group> group) noexcept {
    assert(!group->next);
    group->previous = tail_;
    Group *last = group.get();
    if (tail_)
      tail_->next = std::move(group);
    else
      head_ = std::move(group);
    tail_ = last;
  }

  void InsertBefore(Group &current, std::unique_ptr<Group> group) noexcept {
    auto &link = current.previous ? current.previous->next : head_;
    group->previous = current.previous;
    group->next = std::move(link);
    current.previous = group.get();
    link = std::move(group);
  }

  std::unique_ptr<Group> Remove(Group *group) noexcept {
    auto &link = group->previous ? group->previous->next : head_;
    auto result = std::move(link);
    link = std::move(result->next);
    if (link)
      link->previous = result->previous;
    else
      tail_ = result->previous;
    result->previous = nullptr;
    return result;
  }

  // Allocation failure must not lose or duplicate a task range. Construct the
  // new tree/descriptor before committing the nonthrowing split.
  bool Offer(Group &left, bool initial) {
    try {
      auto siblings = std::make_shared<Siblings>(left.branch);
      auto left_branch = std::make_shared<Branch>(siblings);
      auto right_branch = std::make_shared<Branch>(siblings);
      auto right = std::make_unique<Group>(
          left.storage, Range{0, 0}, left.partition, left.origin);
      if (initial) {
        right->range = Range(left.range, Split{});
        right->partition = Partition(left.partition, Split{});
      } else {
        right->range = left.ranges->Front();
        right->partition = Partition(left.partition, Split{});
        right->partition.AlignDepth(left.ranges->FrontDepth());
        left.ranges->PopFront();
      }
      left.branch = std::move(left_branch);
      right->branch = std::move(right_branch);
      InsertBefore(left, std::move(right));
      offers_.fetch_add(1, std::memory_order_relaxed);
      return true;
    } catch (const std::bad_alloc &) {
      return false;
    }
  }

  Acquired TakeOne(int worker, bool published) {
    Group &group = *tail_;
    if (!group.started) {
      bool live_peer = group.branch &&
          group.branch->parent->live.load(std::memory_order_relaxed) >= 2;
      if (group.partition.CheckBeingStolen(
              Policy::feedback && group.origin != worker, live_peer)) {
        group.branch->parent->stolen.store(true, std::memory_order_relaxed);
        feedback_.fetch_add(1, std::memory_order_relaxed);
      }
      group.started = true;
    }
    group.origin = worker;

    if (!group.ranges) {
      while (group.range.IsDivisible()) {
        // IsDivisible can consume the final balancing-task budget. Roll back
        // that decision if allocating the corresponding sibling fails.
        const Partition before = group.partition;
        if (!group.partition.IsDivisible())
          break;
        if (!Offer(group, true)) {
          group.partition = before;
          break;
        }
        published = true;
      }
      group.ranges.emplace(group.range);
    }

    for (;;) {
      group.ranges->Fill(group.partition.MaxDepth());
      const bool stolen = Policy::feedback && group.branch &&
          group.branch->parent->stolen.load(std::memory_order_relaxed);
      const Partition before = group.partition;
      if (!group.partition.CheckForDemand(stolen))
        break;
      if (group.ranges->Size() > 1) {
        if (Offer(group, false)) {
          published = true;
          continue;
        }
        group.partition = before;
        break;
      }
      if (!group.ranges->IsDivisible(group.partition.MaxDepth()))
        break;
    }

    Range &range = group.ranges->Back();
    Acquired result{group.storage->tasks[range.begin++], group.branch, published};
    if (range.begin == range.end)
      group.ranges->PopBack();
    if (group.ranges->Empty())
      Remove(&group);
    return result;
  }

  std::mutex mutex_;
  std::unique_ptr<Group> head_;
  Group *tail_ = nullptr;
  std::atomic<std::size_t> groups_{0};
  std::atomic<std::size_t> offers_{0};
  std::atomic<std::size_t> remote_groups_{0};
  std::atomic<std::size_t> feedback_{0};
  std::atomic<std::size_t> grouped_tasks_{0};
};

} // namespace oox::detail::eigen_pool

#endif // OOX_EIGEN_DEMAND_POLICY_H
