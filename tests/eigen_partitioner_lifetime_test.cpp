// Copyright (c) 2005-2021 Intel Corporation
// SPDX-License-Identifier: Apache-2.0
#include <oox/eigen/parallel_for.h>
#include <oox/eigen/rapid_auto.h>
#include <algorithm>
#include <array>
#include <atomic>
#include <cstdlib>
#include <iostream>
#include <new>

namespace {
std::atomic<void *> watched{nullptr};
std::atomic<unsigned> regions_destroyed{0};
}

void *operator new(size_t size) {
  if (void *memory = std::malloc(size ? size : 1))
    return memory;
  throw std::bad_alloc();
}
void operator delete(void *memory) noexcept {
  if (memory && memory == watched.load())
    ++regions_destroyed;
  std::free(memory);
}
void operator delete(void *memory, size_t) noexcept { ::operator delete(memory); }

using namespace oox::detail::eigen_pool;

namespace {
struct Owner : partitioner_detail::PeerJoin<Owner> {
  Owner(PeerJoin *parent, unsigned bit, unsigned &destroyed)
      : PeerJoin(parent), bit(bit), destroyed(destroyed) {}
  ~Owner() { destroyed |= bit; }
  std::thread::id PublishingThreadId() const noexcept { return publisher; }
  const std::thread::id publisher = std::this_thread::get_id();
  unsigned bit;
  unsigned &destroyed;
};
}

int main() {
  ThreadPool pool(1, false, false);
  std::array<unsigned, 8> order{0, 1, 2, 3, 4, 5, 6, 7};
  size_t cases = 0;
  do {
    watched = nullptr;
    regions_destroyed = 0;
    unsigned destroyed = 0;
    std::array<bool, 8> seen{};
    auto *region = NewSmallObject<partitioner_detail::Region>(pool, nullptr);
    region->AddTask();
    watched = region;
    auto *a = NewSmallObject<Owner>(Owner::Root(*region), 1, destroyed);
    auto *b = NewSmallObject<Owner>(a, 2, destroyed);
    auto *c = NewSmallObject<Owner>(b, 4, destroyed);
    for (unsigned event : order) {
      seen[event] = true;
      switch (event) {
      case 0: a->FinishExecution(); break;
      case 1: b->FinishExecution(); break;
      case 2: c->FinishExecution(); break;
      case 3: Owner::Release(a); break;
      case 4: Owner::Release(b); break;
      case 5: Owner::Release(c); break;
      case 6: Owner::Release(c); break;
      case 7:
        region->CloseAndWait();
        region->TaskComplete();
        break;
      }
      // Flat Boolean oracle: no tagged pointers or reference-count simulation.
      const bool c_done = seen[5] && seen[6];
      const bool b_done = seen[4] && c_done;
      const bool a_done = seen[3] && b_done;
      const unsigned expected = (seen[0] && a_done ? 1 : 0) |
          (seen[1] && b_done ? 2 : 0) | (seen[2] && c_done ? 4 : 0);
      if (destroyed != expected ||
          regions_destroyed.load() != unsigned(seen[7] && a_done) ||
          (!seen[7] && region->IsComplete() != a_done)) {
        std::cerr << "case=" << cases << " event=" << event
                  << " tree/root lifetime mismatch\n";
        return 1;
      }
    }
    ++cases;
  } while (std::next_permutation(order.begin(), order.end()));
  for (bool discard : {false, true}) {
    watched = nullptr;
    regions_destroyed = 0;
    bool called = false;
    auto body = [&](size_t) { called = true; };
    auto callback = std::make_unique<decltype(body)>(body);
    rapid::RapidDomainState domain(pool);
    using Launch = rapid::batch_detail::Launch<decltype(body)>;
    using Work = Launch::Work;
    using Join = Launch::Join;
    auto *region = NewSmallObject<partitioner_detail::Region>(pool, nullptr, true);
    watched = region;
    Work *held = nullptr;
    {
      Launch launch(rapid::RapidStartGroup{&domain, {0, 1}}, *region, callback.get());
      launch.Prepare({0, 2, 1}, 1);
      const auto seed = launch.GetSeed(0);
      if (!region->IsComplete() || seed.parent ||
          seed.state.unpublished != &region->RapidPublication()) return 9;
      held = NewSmallObject<Work>(*region, callback.get(), seed.range,
                                 seed.state, 0, seed.parent);
      if (region->IsComplete() || held->parent != Join::Root(*region)) return 10;
      Join::Release(held); // The captured owner half finished.
      region->CloseAndWait();
      launch.Run(1); // The other owner declines callback admission.
    } // The actual stack frame ends before the callback and delayed task.
    callback.reset();
    region->TaskComplete();
    if (regions_destroyed.load() != 0) return 4;
    if (discard) held->Discard(); else (*held)();
    if (called || regions_destroyed.load() != 1) return 5;
  }
  for (bool discard : {false, true}) {
    watched = nullptr;
    regions_destroyed = 0;
    ThreadPool donation_pool(2, false, false);
    rapid::RapidDomainState domain(donation_pool);
    bool called = false;
    auto body = [&](size_t) { called = true; };
    auto callback = std::make_unique<decltype(body)>(body);
    auto *function = callback.get();
    using Launch = rapid::batch_detail::Launch<decltype(body)>;
    using Work = Launch::Work;
    auto *region = NewSmallObject<partitioner_detail::Region>(donation_pool, nullptr, true);
    watched = region;
    Work *held = nullptr;
    {
      Launch launch(rapid::RapidStartGroup{&domain, {0, 2}}, *region, function);
      launch.Prepare({0, 128, 1}, 1);
      auto seed = launch.GetSeed(0);
      if (!region->IsComplete() || seed.parent ||
          seed.state.unpublished != &region->RapidPublication()) return 11;
      partitioner_detail::LoopRange right(seed.range, partitioning::Split{});
      if (!region->ReserveDonation()) return 6;
      held = NewSmallObject<Work>(*region, function, right,
          Launch::State(seed.state, partitioning::Split{}), 0, seed.parent, true);
      if (region->IsComplete()) return 12;
      Launch::Join::Release(held); // The donor has finished its half.
      region->CloseAndWait();
      launch.Run(1);
    }
    callback.reset();
    region->TaskComplete();
    if (regions_destroyed.load() != 0) return 7;
    if (discard) held->Discard(); else (*held)();
    if (called || regions_destroyed.load() != 1) return 8;
  }
  for (bool discard : {false, true}) {
    watched = nullptr;
    regions_destroyed = 0;
    bool called = false;
    auto body = [&](size_t) { called = true; };
    auto callback = std::make_unique<decltype(body)>(body);
    rapid::RapidDomainState domain(pool);
    using Launch = rapid::batch_detail::Launch<decltype(body)>;
    using Work = Launch::Work;
    using Join = Launch::Join;
    auto *region = NewSmallObject<partitioner_detail::Region>(pool, nullptr, true);
    watched = region;
    Work *last = nullptr;
    {
      Launch launch(rapid::RapidStartGroup{&domain, {0, 1}}, *region, callback.get());
      launch.Prepare({0, 128, 1}, 0);
      const auto seed = launch.GetSeed(0);
      if (!region->IsComplete() || seed.parent) return 13;
      auto *first = NewSmallObject<Work>(*region, callback.get(), seed.range,
                                       seed.state, 0, seed.parent);
      if (region->IsComplete() || first->parent != Join::Root(*region)) return 14;
      last = NewSmallObject<Work>(*region, callback.get(), seed.range, seed.state, 0, first);
      Join::Release(first); // The captured owner finishes after first promotion.
      Join::Release(last); // The first task's local half has also finished.
      first->FinishExecution();
      region->CloseAndWait();
      if (region->IsComplete()) return 15;
    }
    callback.reset();
    region->TaskComplete(); // Exactly one Region reference protects the whole tree.
    if (regions_destroyed.load() != 0) return 16;
    if (discard) last->Discard(); else (*last)();
    if (called || regions_destroyed.load() != 1) return 17;
  }
  for (bool discard : {false, true}) {
    watched = nullptr;
    regions_destroyed = 0;
    bool called = false;
    auto body = [&](size_t) { called = true; };
    auto callback = std::make_unique<decltype(body)>(body);
    using State = partitioning::AffinityPartitionState;
    using Work = partitioner_detail::Work<decltype(body), State>;
    using Join = partitioner_detail::PeerJoin<Work>;
    auto *region = NewSmallObject<partitioner_detail::Region>(pool, nullptr, true);
    watched = region;
    region->AddTask(); // One native Rapid seed's descendant-tree reference.
    std::weak_ptr<partitioning::AffinityHistory> expired;
    Work *held;
    {
      auto history = std::make_shared<partitioning::AffinityHistory>(2);
      expired = history;
      State root(std::move(history), 0);
      State child(root, partitioning::ProportionalSplit{1, 1});
      held = NewSmallObject<Work>(*region, callback.get(),
          partitioner_detail::LoopRange{0, 128, 1}, child, 0, Join::Root(*region));
      Join::Release(held); // Initial owner's local half has finished.
      region->CloseAndWait();
    }
    callback.reset();
    region->TaskComplete();
    if (!expired.expired() || regions_destroyed.load() != 0) return 18;
    // Must reject admission before NoteExecution touches the freed history.
    if (discard) held->Discard(); else (*held)();
    if (called || regions_destroyed.load() != 1) return 19;
  }
  watched = nullptr;
  std::cout << "root/owner lifecycle orders=" << cases << " PASS\n";
  std::cout << "stack launch late completion cases=6 single Region reference=PASS\n";
}
