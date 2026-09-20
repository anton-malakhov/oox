// Copyright (c) 2005-2021 Intel Corporation
// SPDX-License-Identifier: Apache-2.0
#include <oox/eigen/parallel_for.h>
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
  watched = nullptr;
  std::cout << "root/owner lifecycle orders=" << cases << " PASS\n";
}
