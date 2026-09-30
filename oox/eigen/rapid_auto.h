// SPDX-License-Identifier: Apache-2.0
#pragma once

#include "parallel_for.h"
#include "rapid_start.h"

namespace oox::detail::eigen_pool::rapid {
namespace batch_detail {

// Both halves are indivisible under the requested grain. There can be no
// descendant tasks borrowing this descriptor, so joining the one captured
// helper suffices even on cancellation; no heap completion tree is needed.
template <typename F>
bool TryTerminalPair(RapidStartGroup group, size_t begin, size_t end, F &function,
                     size_t grain, partitioner_detail::Metrics *metrics, size_t *activated) {
  auto &pool = group.state->Pool();
  partitioner_detail::LoopRange left(begin, end, grain);
  const partitioner_detail::LoopRange right(left, partitioning::Split{});
  assert(!left.IsDivisible() && !right.IsDivisible());
  auto body = [&](size_t slot, size_t) {
    const auto &range = slot == 0 ? left : right;
    pool.RunInlineWork([&] {
      if (metrics) metrics->owner_ranges.fetch_add(1, std::memory_order_relaxed);
      for (size_t i = range.begin; i < range.end; ++i) std::invoke(function, i);
      if (metrics) metrics->private_chunks.fetch_add(1, std::memory_order_relaxed);
    });
  };
  ResidentRegion<decltype(body)> region(*group.state, body, 0, 2, 2, 1);
  unsigned worker = 0;
  if (!pool.ClaimResidentWorkers(group.domain, &worker, 1)) return false;
#ifdef OOX_EIGEN_TEST_RAPID_CLAIMED
  OOX_EIGEN_TEST_RAPID_CLAIMED(1);
#endif
  pool.PublishResident(region, region.CompletionCounter(), worker, 1);
  region.RunCaller();
  pool.HelpResidentUntil(region);
  region.RethrowAfterJoin();
  if (activated) *activated = 1;
  return true;
}

template <typename F, size_t Participants = 64>
class Launch final : public partitioner_detail::PinnedRoot, public ResidentTask {
  static_assert(Participants >= 2 && Participants <= 64);
public:
  using State = partitioning::AutoPartitionState;
  using Work = partitioner_detail::Work<F, State>;
  using Join = partitioner_detail::PeerJoin<Work>;
  using Range = partitioner_detail::LoopRange;
  struct Seed {
    Range range;
    State state;
    Join *parent;
    unsigned owner = 0;
    int next = -1;
    bool queued = false, task_constructed = false;
  };
  class InitialTask final : public Task {
  public:
    InitialTask(Launch &launch, size_t seed) noexcept
        : launch_(&launch), seed_(seed), owner_(std::this_thread::get_id()) {}
    void operator()() final {
      auto *launch = launch_;
      launch->RunSeed(seed_, owner_);
      launch->Release();
    }
    void Discard() noexcept final {
      auto *launch = launch_;
      Join::Release(launch->seeds_[seed_].value.parent);
      launch->Release();
    }
  private:
    Launch *launch_;
    size_t seed_;
    std::thread::id owner_;
  };

  Launch(RapidStartGroup group, partitioner_detail::Region &region, F *function) noexcept
      : PinnedRoot{FinishTree}, group_(group), region_(region), function_(function) {}
  ~Launch() {
    for (size_t i = 0; i < node_count_; ++i) {
      // The execution bit pins prefix storage; no task executes these nodes.
      assert(nodes_[i].value.references.load(std::memory_order_relaxed) == Join::executing);
      std::destroy_at(&nodes_[i].value);
    }
    for (size_t i = 0; i < seed_count_; ++i) {
      if (seeds_[i].value.task_constructed) std::destroy_at(&tasks_[i].value);
      std::destroy_at(&seeds_[i].value);
    }
  }

  // The prefix-only form is used by structural tests; production expands it.
  void Prepare(Range range, size_t helpers, bool expand = false) noexcept {
    assert(seed_count_ == 0 && helpers < Participants);
    remaining_.store(helpers, std::memory_order_relaxed);
    region_.AddTask();
    std::construct_at(&seeds_[0].value,
        Seed{range, AutoPartitioner{}.MakeState(region_.pool), Join::Root(*this)});
    seed_count_ = 1;
    // After h splits there are h+1 leaves and 2*h+1 FIFO entries, with
    // h <= helpers < Participants. Each owner expands to at most four leaves
    // (two queued), so the full tree has at most 4*Participants-1 join nodes.
    // Breadth-first prefix construction: O(participants), no per-node allocation.
    unsigned pending[2 * Participants - 1];
    size_t head = 0, tail = 1;
    pending[0] = 0;
    while (seed_count_ < helpers + 1 && head < tail) {
      const unsigned index = pending[head++];
      auto &left = seeds_[index].value;
      if (!left.range.IsDivisible() || left.state.divisor <= 1) continue;
      const auto split = left.state.GetSplit();
      Range right(left.range, split);
      State right_state(left.state, split);
      auto *join = std::construct_at(&nodes_[node_count_++].value, left.parent);
      left.parent = join;
      std::construct_at(&seeds_[seed_count_].value, Seed{right, right_state, join});
      pending[tail++] = index;
      pending[tail++] = static_cast<unsigned>(seed_count_++);
    }
    for (size_t i = 0; i < helpers + 1; ++i) heads_[i] = -1;
    for (size_t i = 0; i < seed_count_; ++i) seeds_[i].value.owner = static_cast<unsigned>(i);
    // Consume local initial fanout only when this branch has at most one
    // worker's Auto budget. Larger budgets must still spread through ordinary
    // Auto; flattening them here concentrates too many small steals on a few
    // initial owners. Each expanded owner needs at most four leaf records.
    if (expand) {
      for (size_t i = 0; i < seed_count_; ++i) {
        auto &left = seeds_[i].value;
        if (left.state.divisor > State(1).divisor) continue;
        while (left.range.IsDivisible() && left.state.IsDivisible()) {
          const auto split = left.state.GetSplit();
          Range right(left.range, split);
          State right_state(left.state, split);
          assert(node_count_ < nodes_.size() && seed_count_ < seeds_.size());
          auto *join = std::construct_at(&nodes_[node_count_++].value, left.parent);
          left.parent = join;
          left.queued = false;
          std::construct_at(&seeds_[seed_count_++].value,
              Seed{right, right_state, join, left.owner, -1, true});
        }
      }
    }
    unsigned queued = 0;
    for (size_t i = seed_count_; i-- > 0;) {
      auto &seed = seeds_[i].value;
      seed.next = heads_[seed.owner];
      heads_[seed.owner] = static_cast<int>(i);
      queued += seed.queued;
    }
    // Reserve all queue-execution pins once, before any seed is published.
    references_.store(2 + queued, std::memory_order_relaxed);
  }

  void Run(size_t slot) noexcept final {
    ScopedResidentState context(*group_.state);
    region_.pool.RunInlineWork([&] {
      // Expansion creates at most two queued leaves per owner. The existing
      // execution pins cover every constructed task, including inline runs.
      ThreadPool::TaskPtr pending[2];
      size_t pending_count = 0;
      size_t direct_count = 0;
      for (int i = heads_[slot]; i != -1; i = seeds_[i].value.next) {
        auto &seed = seeds_[i].value;
        if (!seed.queued) {
          ++direct_count;
          continue;
        }
        auto *task = std::construct_at(&tasks_[i].value, *this, static_cast<size_t>(i));
        seed.task_constructed = true;
        assert(pending_count < std::size(pending));
        pending[pending_count++] = task;
      }
      if (pending_count) {
#ifdef OOX_EIGEN_TEST_RAPID_BATCH_PUBLICATION
        OOX_EIGEN_TEST_RAPID_BATCH_PUBLICATION(pending_count);
#endif
        try {
          // Batch publication consumes every pointer, including on cancellation
          // and queue-full inline execution. Never release those pins twice.
          region_.pool.RunOnThreadBatch(pending, pending_count,
                                       region_.pool.CurrentThreadId());
        } catch (...) {
          region_.Fail(std::current_exception());
        }
      }
      // Expansion can make two direct leaves for one owner. Their callback
      // lifetime is covered by this lease until both synchronous runs return.
      // Queued seeds above never borrow it, even if they execute on this worker.
      std::optional<partitioner_detail::WorkLease> admission;
      if (direct_count > 1) {
#ifdef OOX_EIGEN_TEST_RAPID_SHARED_ADMISSION
        OOX_EIGEN_TEST_RAPID_SHARED_ADMISSION(direct_count);
#endif
        admission.emplace(region_);
      }
      const auto *shared_admission = admission ? std::addressof(*admission) : nullptr;
      for (int i = heads_[slot]; i != -1; i = seeds_[i].value.next)
        if (!seeds_[i].value.queued) {
#ifdef OOX_EIGEN_TEST_RAPID_DIRECT_SEED
          OOX_EIGEN_TEST_RAPID_DIRECT_SEED(slot, i);
#endif
          RunSeed(i, std::this_thread::get_id(), shared_admission);
        }
    });
  }
  bool IsComplete() const noexcept { return remaining_.load(std::memory_order_acquire) == 0; }
  std::atomic<size_t> &CompletionCounter() noexcept { return remaining_; }
  size_t SeedCount() const noexcept { return seed_count_; }
  const Seed &GetSeed(size_t index) const noexcept { return seeds_[index].value; }
  void Release() noexcept {
    if (references_.fetch_sub(1, std::memory_order_acq_rel) == 1) DeleteSmallObject(this);
  }

private:
  void RunSeed(size_t index, std::thread::id owner,
               const partitioner_detail::WorkLease *admission = nullptr) noexcept {
    const Seed seed = seeds_[index].value;
    partitioner_detail::TaskContext<State> context;
    context.owner = owner;
    if (owner == std::this_thread::get_id()) {
      // This is a continuation after initial division, not a new stolen task.
      partitioner_detail::Process<F, State, false>(region_, function_, seed.range,
          seed.state, seed.parent, context, admission);
    } else {
      assert(!admission); // A stolen task must acquire its own callback lease.
      partitioner_detail::Process<F, State>(region_, function_, seed.range,
          seed.state, seed.parent, context);
    }
  }
  // Construction touches only used records, not the full fixed-capacity arrays.
  union SeedSlot { Seed value; SeedSlot() noexcept {} ~SeedSlot() {} };
  union NodeSlot {
    Join value;
    NodeSlot() noexcept {}
    ~NodeSlot() {}
  };
  union TaskSlot { InitialTask value; TaskSlot() noexcept {} ~TaskSlot() {} };
  static void FinishTree(partitioner_detail::PinnedRoot *root, bool) noexcept {
    auto *launch = static_cast<Launch *>(root);
    // Like ordinary Auto, the caller cannot be parked while completing its
    // own tree. Remote completion must still wake registered/external waits.
    launch->region_.TaskComplete(std::this_thread::get_id() != launch->caller_);
    launch->Release();
  }
  RapidStartGroup group_;
  partitioner_detail::Region &region_;
  F *function_;
  const std::thread::id caller_ = std::this_thread::get_id();
  std::array<SeedSlot, 4 * Participants> seeds_;
  std::array<NodeSlot, 4 * Participants - 1> nodes_;
  std::array<TaskSlot, 4 * Participants> tasks_;
  int heads_[Participants];
  size_t seed_count_ = 0, node_count_ = 0;
  std::atomic<size_t> remaining_{0};
  // One reference for the launch/captured commands, one for the Auto tree.
  std::atomic<unsigned> references_{2};
};

template <size_t Participants, typename F>
void Execute(RapidStartGroup group, size_t begin, size_t end, F &function,
             size_t grain, partitioner_detail::Metrics *metrics,
             size_t *activated, size_t capacity) {
  auto &pool = group.state->Pool();
  const auto release_region = [](partitioner_detail::Region *region) { region->TaskComplete(); };
  std::unique_ptr<partitioner_detail::Region, decltype(release_region)> region(
      NewSmallObject<partitioner_detail::Region>(pool, metrics), release_region);
  using Frame = Launch<F, Participants>;
  // Both allocations precede claiming. Setup and resident publication cannot throw.
  auto *launch = NewSmallObject<Frame>(group, *region, std::addressof(function));
  const auto release_launch = [](Frame *value) { value->Release(); };
  std::unique_ptr<Frame, decltype(release_launch)> owner(launch, release_launch);
  std::array<unsigned, Participants - 1> workers;
  const size_t helpers = pool.ClaimResidentWorkers(group.domain, workers.data(), capacity);
#ifdef OOX_EIGEN_TEST_RAPID_CLAIMED
  OOX_EIGEN_TEST_RAPID_CLAIMED(helpers);
#endif
  launch->Prepare({begin, end, grain}, helpers, true);
  for (size_t i = 0; i < helpers; ++i)
    pool.PublishResident(*launch, launch->CompletionCounter(), workers[i], i + 1);
  launch->Run(0);
  pool.HelpResidentUntil(*launch);
  pool.Wait([&] { return region->IsComplete(); });
  region->CloseAndWait();
  region->Rethrow();
  if (activated) *activated = helpers;
}

} // namespace batch_detail

template <typename F>
void ParallelForAuto(RapidStartGroup group, size_t begin, size_t end,
                     F &&function, size_t grain = 1,
                     partitioner_detail::Metrics *metrics = nullptr,
                     size_t *activated = nullptr) {
  if (activated) *activated = 0;
  if (group.IsEmpty() || begin >= end) return;
  group.Validate();
  auto &pool = group.state->Pool();
  if (!pool.UsesResidentBusyWait())
    throw std::invalid_argument("Rapid auto requires a resident-capable pool");
  if (pool.IsCancelled()) return;
  grain = std::max(grain, size_t{1});
  if (current_resident_state || pool.IsExecutingTask() || pool.NumThreads() == 1 ||
      end - begin <= grain || pool.ResidentAvailableWorkers(group.domain) == 0) {
    // Ordinary Auto executes its root directly on this caller. Preserve the
    // Rapid entry's ancestry even if helpers become available mid-callback.
    return pool.RunInlineWork([&] {
      oox::detail::eigen_pool::ParallelFor(
          pool, begin, end, std::forward<F>(function), grain, metrics);
    });
  }
  // Overflow-safe size <= 2*grain; preserve Auto's floor/ceil midpoint.
  if (end - begin - grain <= grain) {
    if (batch_detail::TryTerminalPair(group, begin, end, function, grain, metrics, activated)) return;
    return pool.RunInlineWork([&] {
      oox::detail::eigen_pool::ParallelFor(
          pool, begin, end, std::forward<F>(function), grain, metrics);
    });
  }
  const size_t capacity = std::min({size_t{63}, pool.NumThreads() - 1,
      pool.ResidentCapacity(group.domain), (end - begin - 1) / grain});
  // Eligibility may change after sizing; ClaimResidentWorkers is still bounded
  // by this capacity. No partially captured launch can overrun its frame.
  if (capacity < 2)
    batch_detail::Execute<2>(group, begin, end, function, grain, metrics, activated, capacity);
  else if (capacity < 8)
    batch_detail::Execute<8>(group, begin, end, function, grain, metrics, activated, capacity);
  else if (capacity < 16)
    batch_detail::Execute<16>(group, begin, end, function, grain, metrics, activated, capacity);
  else
    batch_detail::Execute<64>(group, begin, end, function, grain, metrics, activated, capacity);
}

} // namespace oox::detail::eigen_pool::rapid
