// SPDX-License-Identifier: Apache-2.0
#include <atomic>
#include <cstdlib>
#include <future>
#include <iostream>
#include <oox/eigen/patent_parallel_for.h>
#include <random>
#include <vector>

using namespace oox::detail::eigen_pool;
using namespace std::chrono_literals;
constexpr uint64_t seed = 0x504154454e543031ULL;
void check(bool ok, const char *why, size_t c = 0) {
  if (!ok) {
    std::cerr << "seed=" << seed << " case=" << c << " " << why << "\n";
    std::_Exit(1);
  }
}
uint64_t reference(size_t i) {
  return (uint64_t(i) + 17) * 0x9e3779b97f4a7c15ULL;
}
void verify(rapid::RapidStartGroup group, size_t n, size_t begin, size_t grain,
            size_t c) {
  std::vector<std::atomic<unsigned>> visits(n);
  std::vector<std::atomic<uint64_t>> output(n);
  rapid::ParallelForPatent(
      group, begin, begin + n,
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
int main() {
  std::mt19937_64 random(seed);
  size_t cases = 0, items = 0;
  for (unsigned p : {1u, 2u, 3u, 8u, 12u}) {
    ThreadPool pool(p, true, true);
    rapid::RapidDomainState state(pool);
    rapid::RapidStartGroup group{&state, {0, p}};
    for (size_t n = 0; n <= 257; ++n) {
      size_t first = (n % 3 == 0) ? std::numeric_limits<size_t>::max() - n : 17;
      verify(group, n, first, 1 + (random() % 23), cases++);
      items += n;
    }
    for (size_t n : {1023ul, 1024ul, 1025ul, 4097ul, 1000003ul}) {
      verify(group, n, 0, 1, cases++);
      items += n;
    }
    for (size_t outer : {1ul, 2ul, 3ul, 7ul, 13ul, 31ul}) {
      constexpr size_t inner = 43;
      std::vector<std::atomic<unsigned>> visits(outer * inner);
      rapid::ParallelForPatent(group, 0, outer, [&](size_t i) {
        rapid::ParallelForPatent(group, 0, inner, [&](size_t j) {
          visits[i * inner + j].fetch_add(1, std::memory_order_relaxed);
        });
      });
      for (auto &v : visits)
        check(v == 1, "nested visitation", cases);
      ++cases;
    }
    bool caught = false;
    try {
      rapid::ParallelForPatent(group, 0, 4097, [&](size_t i) {
        if (i == 13)
          throw std::runtime_error("expected");
      });
    } catch (const std::runtime_error &) {
      caught = true;
    }
    check(caught, "exception not propagated", cases++);
    verify(group, 129, 0, 1, cases++);
    if (p > 1) {
      std::vector<std::future<void>> roots;
      for (unsigned t = 0; t < 4; ++t)
        roots.push_back(std::async(std::launch::async, [&, t] {
          for (unsigned r = 0; r < 8; ++r)
            verify(group, 257 + t * 17, 0, 1, 100000 + t * 8 + r);
        }));
      for (auto &r : roots) {
        check(r.wait_for(15s) == std::future_status::ready,
              "concurrent root hang");
        r.get();
      }
      cases += 32;
    }
  }
  for (unsigned p : {2u, 8u, 12u}) {
    ThreadPool pool(p, true, true);
    rapid::RapidDomainState scarce(pool, 0);
    rapid::RapidStartGroup group{&scarce, {0, p}};
    verify(group, 16387, 0, 1, cases++);
    items += 16387;
  }
  {
    ThreadPool pool(8, true, true), other(3, true, false);
    rapid::RapidDomainState a(pool), b(other);
    rapid::RapidStartGroup ga{&a, {0, 8}}, gb{&b, {0, 3}};
    std::vector<std::atomic<unsigned>> visits(7 * 37);
    rapid::ParallelForPatent(ga, 0, 7, [&](size_t i) {
      rapid::ParallelForPatent(gb, 0, 37,
                               [&](size_t j) { ++visits[i * 37 + j]; });
    });
    for (auto &v : visits)
      check(v == 1, "cross-pool visitation", cases);
    ++cases;
  }
  for (unsigned p : {2u, 8u, 12u}) {
    auto done = std::async(std::launch::async, [p] {
      ThreadPool pool(p, true, true);
      rapid::RapidDomainState state(pool);
      rapid::RapidStartGroup group{&state, {0, p}};
      std::atomic<bool> once{false};
      rapid::ParallelForPatent(group, 0, 65536, [&](size_t) {
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
    ThreadPool pool(8, true, true);
    rapid::RapidDomainState state(pool);
    rapid::RapidStartGroup group{&state, {0, 8}};
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
                          [&] { verify(group, 16387, 0, 1, cases); });
    const bool completed = run.wait_for(15s) == std::future_status::ready;
    release.store(true);
    while (finished.load() != 6)
      std::this_thread::yield();
    check(completed, "unavailable-worker progress", cases);
    run.get();
    ++cases;
  }
  std::cout << "{\"seed\":" << seed << ",\"cases\":" << cases
            << ",\"oracle_items\":" << items
            << ",\"nested\":true,\"concurrent_roots\":true,\"cross_pool\":true,"
               "\"scarcity\":true,\"cancellation\":true,\"failures\":0}\n";
}
