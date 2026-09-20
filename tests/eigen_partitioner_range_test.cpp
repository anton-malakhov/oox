// SPDX-License-Identifier: Apache-2.0
#include <array>
#include <atomic>
#include <cstdlib>
#include <future>
#include <iostream>
#include "eigen_partitioner_test_support.h"
#include <random>
#include <vector>

using namespace oox::detail::eigen_pool;
using namespace std::chrono_literals;
constexpr uint64_t seed = 0x504154454e543031ULL;
static_assert(std::is_empty_v<
              partitioner_detail::TaskContext<partitioning::SimplePartitionState>>);
static_assert([] {
  partitioning::StaticPartitionState state(3, 0);
  partitioner_detail::LoopRange left(10, 27, 1);
  const auto split = state.GetSplit();
  partitioner_detail::LoopRange right(left, split);
  partitioning::StaticPartitionState child(state, split);
  return left.begin == 10 && left.end == 21 && right.begin == 21 &&
         right.end == 27 && state.divisor == 2 && child.divisor == 1 &&
         child.Hint() == 2;
}());
static_assert([] {
  partitioning::AutoPartitionState state(8);
  while (state.divisor > 1) {
    partitioning::AutoPartitionState child(state, partitioning::Split{});
  }
  return state.IsDivisible() && state.divisor == 0 && state.max_depth == 4;
}());
static_assert([] {
  partitioning::RangePool<partitioner_detail::LoopRange> ranges({0, 16, 1});
  ranges.Fill(2);
  return ranges.Size() == 3 && ranges.Back().begin == 0 &&
         ranges.Back().end == 4 && ranges.Front().begin == 8 &&
         ranges.Front().end == 16 && ranges.FrontDepth() == 1;
}());
void check(bool ok, const char *why, size_t c = 0) {
  if (!ok) {
    std::cerr << "seed=" << seed << " case=" << c << " " << why << "\n";
    std::_Exit(1);
  }
}
void verify_join_lifetimes() {
  struct Owner : partitioner_detail::PeerJoin<Owner> {
    Owner(Owner *parent, unsigned bit, unsigned &destroyed)
        : PeerJoin(parent), bit(bit), destroyed(destroyed) {}
    ~Owner() { destroyed |= bit; }
    unsigned bit;
    unsigned &destroyed;
  };
  std::array<unsigned, 7> order{0, 1, 2, 3, 4, 5, 6};
  size_t case_index = 0;
  do {
    unsigned destroyed = 0;
    auto *a = NewSmallObject<Owner>(nullptr, 1, destroyed);
    auto *b = NewSmallObject<Owner>(a, 2, destroyed);
    auto *c = NewSmallObject<Owner>(b, 4, destroyed);
    std::array<bool, 7> seen{};
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
      }
      // Independent Boolean oracle for the three-node dependency tree.
      const bool c_done = seen[5] && seen[6];
      const bool b_done = seen[4] && c_done;
      const bool a_done = seen[3] && b_done;
      const unsigned expected = (seen[0] && a_done ? 1u : 0u) |
                                (seen[1] && b_done ? 2u : 0u) |
                                (seen[2] && c_done ? 4u : 0u);
      check(destroyed == expected, "embedded join lifetime order", case_index);
      if (!(destroyed & 1))
        check(a->HasPeer() == (!seen[3] && !b_done), "root peer state", case_index);
      if (!(destroyed & 2))
        check(b->HasPeer() == (!seen[4] && !c_done), "middle peer state", case_index);
      if (!(destroyed & 4))
        check(c->HasPeer() == (!seen[5] && !seen[6]), "leaf peer state", case_index);
    }
    ++case_index;
  } while (std::next_permutation(order.begin(), order.end()));
  check(case_index == 5040, "join lifetime permutation coverage");
}

uint64_t reference(size_t i) {
  return (uint64_t(i) + 17) * 0x9e3779b97f4a7c15ULL;
}
void verify(ThreadPool &pool, size_t n, size_t begin, size_t grain,
            size_t c) {
  std::vector<std::atomic<unsigned>> visits(n);
  std::vector<std::atomic<uint64_t>> output(n);
  eigen_partitioner_test::Run(
      pool, begin, begin + n,
      [&](size_t i) {
        check(i >= begin && i < begin + n, "out of range", c);
        const auto index = i - begin;
        visits[index].fetch_add(1, std::memory_order_relaxed);
        output[index].store(reference(index), std::memory_order_relaxed);
      },
      grain);
  for (size_t i = 0; i < n; ++i)
    check(visits[i] == 1 && output[i] == reference(i), "serial oracle mismatch",
          c);
}
void verify_affinity_history() {
  ThreadPool pool(3, false, true);
  AffinityPartitioner affinity;
  std::atomic<size_t> *remembered = nullptr;
  {
    auto state = affinity.MakeState(pool);
    remembered = state.slots;
    partitioning::AffinityPartitionState child(
        state, partitioning::ProportionalSplit{2, 1});
    check(child.Hint() == 2, "initial affinity placement");
    child.NoteExecution(1, 2);
    check(child.Hint() == 1, "affinity did not learn execution worker");
    auto overlapping = affinity.MakeState(pool);
    check(overlapping.slots != remembered, "overlapping affinity history shared");
  }
  {
    auto state = affinity.MakeState(pool);
    partitioning::AffinityPartitionState child(
        state, partitioning::ProportionalSplit{2, 1});
    check(state.slots == remembered && child.Hint() == 1,
          "sequential affinity history not retained");
  }
  ThreadPool other(2, false, false);
  auto reset = affinity.MakeState(other);
  check(reset.slots_count == 32, "affinity history wrong pool size");
  for (size_t i = 0; i < reset.slots_count; ++i)
    check(reset.slots[i] == 0, "affinity history not reset across pools");
}

void verify_cancelled_affinity_lifetime() {
  ThreadPool pool(2, false, true);
  std::atomic<bool> entered{false}, release{false};
  pool.RunOnThread(MakeTask([&] {
    entered.store(true); entered.notify_one();
    release.wait(false);
  }), 1);
  entered.wait(false);
  std::atomic<unsigned> visits{0};
  auto run = std::async(std::launch::async, [&] {
    AffinityPartitioner temporary;
    ParallelFor(pool, 0, 4097, [&](size_t) {
      ++visits;
      pool.Cancel();
    }, temporary);
  });
  check(run.wait_for(15s) == std::future_status::ready,
        "cancelled affinity root hang");
  run.get();
  const auto at_return = visits.load();
  // The callback and temporary history are gone; queued tasks remain because
  // the external caller cannot help and the only background worker is blocked.
  pool.Wait([] { return false; });
  check(visits.load() == at_return && at_return != 0,
        "queued affinity task accessed expired callback");
  release.store(true); release.notify_one();
}

int main(int argc, char **argv) {
  eigen_partitioner_test::Select(argc, argv);
  verify_join_lifetimes();
  if (eigen_partitioner_test::policy == eigen_partitioner_test::Policy::Affinity) {
    verify_affinity_history();
    verify_cancelled_affinity_lifetime();
  }
  std::mt19937_64 random(seed);
  size_t cases = 0, items = 0;
  for (unsigned p : {1u, 2u, 3u, 8u, 12u}) {
    ThreadPool pool(p, true, true);
    for (size_t n : {1ul, 2ul, 7ul, 31ul, 257ul}) {
      partitioner_detail::Metrics metrics;
      std::vector<std::atomic<unsigned>> visits(n);
      eigen_partitioner_test::Run(pool, 0, n, [&](size_t i) { ++visits[i]; },
                        1, &metrics);
      for (auto &v : visits)
        check(v == 1, "initial subdivision oracle", cases);
      check(metrics.range_tasks == metrics.initial_tasks +
                                       metrics.donated_ranges &&
                metrics.range_tasks < n,
            "nonempty range task accounting", cases++);
      items += n;
    }
    for (size_t n = 0; n <= 257; ++n) {
      size_t first = (n % 3 == 0) ? std::numeric_limits<size_t>::max() - n : 17;
      verify(pool, n, first, 1 + (random() % 23), cases++);
      items += n;
    }
    for (size_t n : {1023ul, 1024ul, 1025ul, 4097ul, 1000003ul}) {
      verify(pool, n, 0, 1, cases++);
      items += n;
    }
    for (size_t outer : {1ul, 2ul, 3ul, 7ul, 13ul, 31ul}) {
      constexpr size_t inner = 43;
      std::vector<std::atomic<unsigned>> visits(outer * inner);
      eigen_partitioner_test::Run(pool, 0, outer, [&](size_t i) {
        eigen_partitioner_test::Run(pool, 0, inner, [&](size_t j) {
          visits[i * inner + j].fetch_add(1, std::memory_order_relaxed);
        });
      });
      for (auto &v : visits)
        check(v == 1, "nested visitation", cases);
      ++cases;
    }
    bool caught = false;
    try {
      eigen_partitioner_test::Run(pool, 0, 4097, [&](size_t i) {
        if (i == 13)
          throw std::runtime_error("expected");
      });
    } catch (const std::runtime_error &) {
      caught = true;
    }
    check(caught, "exception not propagated", cases++);
    verify(pool, 129, 0, 1, cases++);
    if (p > 1) {
      std::vector<std::future<void>> roots;
      for (unsigned t = 0; t < 4; ++t)
        roots.push_back(std::async(std::launch::async, [&, t] {
          for (unsigned r = 0; r < 8; ++r)
            verify(pool, 257 + t * 17, 0, 1, 100000 + t * 8 + r);
        }));
      for (auto &r : roots) {
        check(r.wait_for(15s) == std::future_status::ready,
              "concurrent root hang");
        r.get();
      }
      cases += 32;
    }
  }
  {
    ThreadPool pool(8, true, true), other(3, true, false);
    std::vector<std::atomic<unsigned>> visits(7 * 37);
    eigen_partitioner_test::Run(pool, 0, 7, [&](size_t i) {
      eigen_partitioner_test::Run(other, 0, 37,
                        [&](size_t j) { ++visits[i * 37 + j]; });
    });
    for (auto &v : visits)
      check(v == 1, "cross-pool visitation", cases);
    ++cases;
  }
  for (unsigned p : {2u, 8u, 12u}) {
    auto done = std::async(std::launch::async, [p] {
      ThreadPool pool(p, true, true);
      std::atomic<bool> once{false};
      eigen_partitioner_test::Run(pool, 0, 65536, [&](size_t) {
        if (!once.exchange(true))
          pool.Cancel();
      });
    });
    check(done.wait_for(15s) == std::future_status::ready, "cancellation hang",
          cases);
    done.get();
    ++cases;
  }
  {
    ThreadPool pool(2, true, true);
    std::atomic<bool> entered{false}, release{false};
    pool.RunOnThread(MakeTask([&] {
      entered.store(true);
      while (!release.load())
        std::this_thread::yield();
    }), 1);
    while (!entered.load())
      std::this_thread::yield();
    std::atomic<unsigned> callbacks{0};
    {
      auto body = [&](size_t) { ++callbacks; pool.Cancel(); };
      eigen_partitioner_test::Run(pool, 0, 65536, body);
    }
    const unsigned at_return = callbacks.load();
    for (unsigned i = 0; i < 4; ++i)
      pool.Wait([] { return false; });
    release.store(true);
    check(at_return != 0 && callbacks.load() == at_return,
          "cancelled queued task accessed expired callback", cases++);
  }
  {
    ThreadPool pool(2, true, false);
    std::atomic<bool> entered{false}, cancelled{false}, release{false};
    std::atomic<unsigned> visits[2]{};
    auto run = std::async(std::launch::async, [&] {
      eigen_partitioner_test::Run(pool, 0, 2, [&](size_t i) {
        ++visits[i];
        if (i == 1) {
          entered.store(true);
          while (!release.load())
            std::this_thread::yield();
        } else {
          while (!entered.load())
            std::this_thread::yield();
          pool.Cancel();
          cancelled.store(true);
        }
      });
    });
    while (!cancelled.load())
      std::this_thread::yield();
    const bool returned_early = run.wait_for(50ms) == std::future_status::ready;
    release.store(true);
    check(run.wait_for(15s) == std::future_status::ready,
          "active callback cancellation hang", cases);
    run.get();
    check(!returned_early && visits[0] == 1 && visits[1] == 1,
          "operation returned before active callback", cases++);
  }
  {
    ThreadPool pool(8, true, true);
    std::atomic<unsigned> started{0}, finished{0};
    std::atomic<bool> release{false};
    for (unsigned i = 0; i < 6; ++i)
      pool.Schedule(MakeTask([&] {
        started.fetch_add(1);
        while (!release.load())
          std::this_thread::yield();
        finished.fetch_add(1);
      }));
    while (started.load() != 6)
      std::this_thread::yield();
    auto run = std::async(std::launch::async,
                          [&] { verify(pool, 16387, 0, 1, cases); });
    const bool completed = run.wait_for(15s) == std::future_status::ready;
    release.store(true);
    while (finished.load() != 6)
      std::this_thread::yield();
    check(completed, "unavailable-worker progress", cases);
    run.get();
    ++cases;
  }
  {
    ThreadPool pool(2, true, true);
    std::atomic<bool> entered{false}, release{false}, exited{false};
    pool.RunOnThread(MakeTask([&] {
      entered.store(true);
      while (!release.load())
        std::this_thread::yield();
      exited.store(true);
    }), 1);
    while (!entered.load())
      std::this_thread::yield();
    std::atomic<unsigned> visits{0};
    for (unsigned i = 0; i < 4096; ++i)
      pool.RunOnThread(MakeTask([&] { ++visits; }), 0);
    verify(pool, 8193, 0, 1, cases++);
    while (visits.load() != 4096)
      pool.TryExecuteSomething();
    release.store(true);
    while (!exited.load())
      std::this_thread::yield();
  }
  std::cout << "{\"seed\":" << seed << ",\"cases\":" << cases
            << ",\"oracle_items\":" << items
            << ",\"nested\":true,\"concurrent_roots\":true,\"cross_pool\":true,"
               "\"cancellation\":true,\"failures\":0}\n";
}
