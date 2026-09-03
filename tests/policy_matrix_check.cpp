// SPDX-License-Identifier: Apache-2.0
// Standalone policy-matrix correctness check (no gtest). Exercises every
// GrainLaw x VictimPolicy combination for exactly-once visitation across sizes
// and grains, exception propagation and reuse, nesting, cancellation, and loop
// profile warm start.
#include <oox/eigen/rapid_start.h>

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <future>
#include <stdexcept>
#include <thread>
#include <vector>

using namespace oox::detail::eigen_pool;
using namespace oox::detail::eigen_pool::rapid;
using namespace std::chrono_literals;

static int failures = 0;
#define CHECK(cond, ...)                                                       \
  do {                                                                         \
    if (!(cond)) {                                                             \
      ++failures;                                                              \
      std::fprintf(stderr, "FAIL %s:%d: %s ", __FILE__, __LINE__, #cond);      \
      std::fprintf(stderr, __VA_ARGS__);                                       \
      std::fprintf(stderr, "\n");                                              \
    }                                                                          \
  } while (0)

struct Harness {
  explicit Harness(unsigned workers, size_t slots_per_worker = 128)
      : pool(static_cast<int>(workers), false, false),
        state(pool, slots_per_worker), group{&state, {0, workers}} {}
  ThreadPool pool;
  RapidDomainState state;
  RapidStartGroup group;
};

template <GrainLaw Law, VictimPolicy Victims> const char *Name() {
  static const char *laws[] = {"Fixed", "Sqrt", "SqrtCv", "Heartbeat",
                               "FSC", "Factoring", "Guided"};
  static const char *victims[] = {"Linear", "Hierarchical", "MostRemaining"};
  static char buffer[64];
  std::snprintf(buffer, sizeof buffer, "%s/%s",
                laws[static_cast<int>(Law)], victims[static_cast<int>(Victims)]);
  return buffer;
}

template <GrainLaw Law, VictimPolicy Victims> void ExactlyOnce() {
  Harness h(8);
  const char *name = Name<Law, Victims>();
  for (size_t size : {size_t{0}, size_t{1}, size_t{2}, size_t{7}, size_t{8},
                      size_t{9}, size_t{63}, size_t{64}, size_t{65},
                      size_t{1000}, size_t{4096}, size_t{65537}}) {
    for (size_t grain : {size_t{1}, size_t{7}, size_t{64}, size_t{4096}}) {
      constexpr size_t begin = 3;
      std::vector<std::atomic<unsigned>> visits(begin + size + 3);
      ParallelForLazyStealingPolicy<Law, Victims>(
          h.group, begin, begin + size,
          [&](size_t i) { visits[i].fetch_add(1, std::memory_order_relaxed); },
          grain);
      for (size_t i = 0; i < visits.size(); ++i) {
        const unsigned expected = (i >= begin && i < begin + size) ? 1u : 0u;
        if (visits[i].load() != expected) {
          CHECK(false, "%s size=%zu grain=%zu index=%zu got=%u", name, size,
                grain, i, visits[i].load());
          break;
        }
      }
    }
  }
}

template <GrainLaw Law, VictimPolicy Victims> void SkewedWork() {
  // Hyperbolic-like work: item i costs ~N/(i+1) spins. Forces stealing.
  Harness h(8);
  const char *name = Name<Law, Victims>();
  constexpr size_t n = 20000;
  std::vector<std::atomic<unsigned>> visits(n);
  std::atomic<unsigned long long> sink{0};
  ParallelForLazyStealingPolicy<Law, Victims>(
      h.group, 0, n, [&](size_t i) {
        visits[i].fetch_add(1, std::memory_order_relaxed);
        unsigned long long acc = 0;
        for (size_t k = 0; k < 2000 / (i + 1) + 1; ++k) {
          acc += k * 2654435761ULL;
        }
        sink.fetch_add(acc, std::memory_order_relaxed);
      });
  for (size_t i = 0; i < n; ++i) {
    if (visits[i].load() != 1u) {
      CHECK(false, "%s skewed index=%zu got=%u", name, i, visits[i].load());
      break;
    }
  }
}

template <GrainLaw Law, VictimPolicy Victims> void ExceptionsAndReuse() {
  Harness h(8);
  const char *name = Name<Law, Victims>();
  bool threw = false;
  try {
    ParallelForLazyStealingPolicy<Law, Victims>(h.group, 0, 1024,
                                                [](size_t i) {
                                                  if (i == 127) {
                                                    throw std::runtime_error(
                                                        "boom");
                                                  }
                                                });
  } catch (const std::runtime_error &) {
    threw = true;
  }
  CHECK(threw, "%s did not propagate exception", name);
  std::atomic<unsigned> done{0};
  ParallelForLazyStealingPolicy<Law, Victims>(
      h.group, 0, 1024,
      [&](size_t) { done.fetch_add(1, std::memory_order_relaxed); });
  CHECK(done.load() == 1024u, "%s reuse got %u", name, done.load());
}

template <GrainLaw Law, VictimPolicy Victims> void Nested() {
  Harness h(8);
  const char *name = Name<Law, Victims>();
  std::vector<std::atomic<unsigned>> visits(512);
  ParallelForLazyStealingPolicy<Law, Victims>(
      h.group, 0, visits.size(), [&](size_t i) {
        visits[i].fetch_add(1, std::memory_order_relaxed);
        if (i < 8) {
          std::atomic<unsigned> inner{0};
          ParallelForLazyStealingPolicy<Law, Victims>(
              h.group, 0, 16,
              [&](size_t) { inner.fetch_add(1, std::memory_order_relaxed); });
          CHECK(inner.load() == 16u, "%s nested inner got %u", name,
                inner.load());
        }
      });
  for (auto &v : visits) {
    if (v.load() != 1u) {
      CHECK(false, "%s nested outer visit=%u", name, v.load());
      break;
    }
  }
}

template <GrainLaw Law, VictimPolicy Victims> void OneWorker() {
  Harness h(1);
  const char *name = Name<Law, Victims>();
  std::atomic<unsigned> done{0};
  ParallelForLazyStealingPolicy<Law, Victims>(
      h.group, 0, 16, [&](size_t) {
        ParallelForLazyStealingPolicy<Law, Victims>(
            h.group, 0, 16,
            [&](size_t) { done.fetch_add(1, std::memory_order_relaxed); });
      });
  CHECK(done.load() == 256u, "%s one-worker nested got %u", name, done.load());
}

template <GrainLaw Law, VictimPolicy Victims> void Cancellation() {
  Harness h(8);
  const char *name = Name<Law, Victims>();
  std::atomic<bool> entered{false};
  auto result = std::async(std::launch::async, [&] {
    ParallelForLazyStealingPolicy<Law, Victims>(h.group, 0, 10000, [&](size_t) {
      entered.store(true, std::memory_order_release);
      std::this_thread::sleep_for(50us);
    });
  });
  while (!entered.load(std::memory_order_acquire)) {
    std::this_thread::yield();
  }
  h.pool.Cancel();
  CHECK(result.wait_for(5s) == std::future_status::ready,
        "%s cancellation did not complete", name);
}

template <GrainLaw Law, VictimPolicy Victims> void DescriptorScarcity() {
  Harness h(8, 0);
  const char *name = Name<Law, Victims>();
  std::vector<std::atomic<unsigned>> visits(1024);
  ParallelForLazyStealingPolicy<Law, Victims>(
      h.group, 0, visits.size(),
      [&](size_t i) { visits[i].fetch_add(1, std::memory_order_relaxed); });
  for (auto &v : visits) {
    if (v.load() != 1u) {
      CHECK(false, "%s scarcity visit=%u", name, v.load());
      break;
    }
  }
}

template <GrainLaw Law, VictimPolicy Victims> void RunAll() {
  ExactlyOnce<Law, Victims>();
  SkewedWork<Law, Victims>();
  ExceptionsAndReuse<Law, Victims>();
  Nested<Law, Victims>();
  OneWorker<Law, Victims>();
  Cancellation<Law, Victims>();
  DescriptorScarcity<Law, Victims>();
  std::printf("  %-24s ok\n", Name<Law, Victims>());
}

template <GrainLaw Law> void RunVictims() {
  RunAll<Law, VictimPolicy::Linear>();
  RunAll<Law, VictimPolicy::Hierarchical>();
  RunAll<Law, VictimPolicy::MostRemaining>();
}

void ProfileWarmStart() {
  Harness h(8);
  LoopProfile profile;
  CHECK(!profile.IsWarm(), "profile warm before use");
  std::atomic<unsigned> done{0};
  auto body = [&](size_t) {
    unsigned long long acc = 0;
    for (size_t k = 0; k < 200; ++k) {
      acc += k * 2654435761ULL;
    }
    if (acc == 42) {
      done.fetch_add(1);
    }
    done.fetch_add(1, std::memory_order_relaxed);
  };
  ParallelForTimespanLazyStealing(h.group, 0, 100000, body, 1, 0, &profile);
  CHECK(done.load() == 100000u, "profile pass 1 got %u", done.load());
  CHECK(profile.IsWarm(), "profile not warm after first call");
  CHECK(profile.calls.load() == 1u, "profile calls=%zu", profile.calls.load());
  const size_t item_ns = profile.item_ns.load();
  CHECK(item_ns > 0 && item_ns < 1'000'000, "item_ns=%zu", item_ns);
  done.store(0);
  ParallelForTimespanLazyStealing(h.group, 0, 100000, body, 1, 0, &profile);
  CHECK(done.load() == 100000u, "profile pass 2 got %u", done.load());
  CHECK(profile.calls.load() == 2u, "profile calls=%zu", profile.calls.load());
  // Warm start must never exceed a quarter of the owner range.
  LazyRangeCoordinator<decltype(body), GrainLaw::Sqrt, VictimPolicy::Linear>
      warm(h.pool, body, 0, 100000, 8, 1, 0, &profile);
  CHECK(warm.InitialBlock() >= 1 && warm.InitialBlock() <= 100000 / 8 / 4 + 1,
        "warm initial block=%zu", warm.InitialBlock());
  std::printf("  profile warm start        ok (item_ns=%zu block=%zu)\n",
              item_ns, warm.InitialBlock());
}

void ProfileTable() {
  LoopProfileTable<16> table;
  LoopProfile *a = table.Get(12345);
  LoopProfile *b = table.Get(12345);
  LoopProfile *c = table.Get(99999);
  CHECK(a != nullptr && a == b, "table identity");
  CHECK(c != nullptr && c != a, "table distinct keys");
  std::printf("  profile table             ok\n");
}

void ModelUnits() {
  // Existing behaviors preserved.
  CHECK(TimespanBlockSize(16, 16, 10'000, 1024, 1, 80'000) == 128u, "tbs1");
  CHECK(TimespanBlockSize(16, 16, 320'000, 1024, 1, 80'000) == 4u, "tbs2");
  CHECK(TimespanBlockSize(64, 64, 1, 10, 2, 80'000) == 3u, "tbs3");
  CHECK(TimespanTargetNanoseconds(100, 10, 1'000, 10'000, 0, 4) == 10'000u,
        "ttn1");
  CHECK(TimespanTargetNanoseconds(100, 10, 1'000, 10'000, 4, 4) == 7'071u,
        "ttn2");
  // New laws.
  CHECK(SqrtCvTargetNanoseconds(100, 10, 1'000, 10'000, 0, 4, 0.0L) ==
            10'000u,
        "sqrtcv cv=0 equals sqrt");
  const size_t cv1 = SqrtCvTargetNanoseconds(100, 10, 1'000, 10'000, 0, 4, 1.0L);
  CHECK(cv1 == 7'071u, "sqrtcv cv=1 got %zu", cv1);
  CHECK(HeartbeatTargetNanoseconds(49) == 980u, "heartbeat");
  CHECK(FixedSizeChunkItems(1000, 100, 0.0L, 8) == 0, "fsc sigma=0 -> 0");
  CHECK(FixedSizeChunkItems(1000, 100, 50.0L, 1) == 0, "fsc P=1 -> 0");
  const size_t fsc = FixedSizeChunkItems(100'000, 800, 50.0L, 16);
  CHECK(fsc > 1 && fsc < 100'000, "fsc=%zu", fsc);
  // 2/3-power scaling: doubling N*h multiplies K by 2^(2/3) ~ 1.587.
  const size_t fsc2 = FixedSizeChunkItems(200'000, 800, 50.0L, 16);
  const double ratio = static_cast<double>(fsc2) / static_cast<double>(fsc);
  CHECK(ratio > 1.55 && ratio < 1.62, "fsc ratio=%f", ratio);
  CHECK(FactoringChunk(1000, 8, 1) == 63u, "fac2");
  CHECK(GuidedChunk(1000, 8, 1) == 125u, "gss");
  CHECK(FactoringChunk(0, 8, 5) == 5u, "fac2 floor");
  CHECK(VictimCandidate(VictimPolicy::Hierarchical, 4, 1, 8) == 5u, "hv1");
  CHECK(VictimCandidate(VictimPolicy::Hierarchical, 4, 2, 8) == 3u, "hv2");
  CHECK(VictimCandidate(VictimPolicy::Hierarchical, 4, 3, 8) == 6u, "hv3");
  CHECK(VictimCandidate(VictimPolicy::Hierarchical, 0, 2, 8) == 7u, "hv wrap");
  CHECK(VictimCandidate(VictimPolicy::Linear, 7, 1, 8) == 0u, "lin wrap");
  CHECK(VictimCandidate(VictimPolicy::Linear, 0, 8, 8) == 8u, "exhausted");
  RunningStats s;
  for (double x : {10.0, 12.0, 8.0, 11.0, 9.0}) {
    s.Add(x, 4.0L);
  }
  CHECK(s.count == 5 && s.mean > 9.9L && s.mean < 10.1L, "stats mean");
  CHECK(s.ItemCv() > 0.0L, "stats cv");
  CHECK(ItemsForDuration(1000, 10, 1, 500) == 100u, "items for duration");
  CHECK(ItemsForDuration(1000, 10, 1, 50) == 50u, "items clamp to work");
  std::printf("  model units               ok\n");
}

int main() {
  std::printf("policy matrix:\n");
  ModelUnits();
  RunVictims<GrainLaw::Fixed>();
  RunVictims<GrainLaw::Sqrt>();
  RunVictims<GrainLaw::SqrtCv>();
  RunVictims<GrainLaw::Heartbeat>();
  RunVictims<GrainLaw::FixedSizeChunk>();
  RunVictims<GrainLaw::Factoring>();
  RunVictims<GrainLaw::Guided>();
  ProfileWarmStart();
  ProfileTable();
  if (failures) {
    std::fprintf(stderr, "%d failure(s)\n", failures);
    return 1;
  }
  std::printf("all passed\n");
  return 0;
}
