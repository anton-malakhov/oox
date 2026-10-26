// SPDX-License-Identifier: Apache-2.0
#include "experiment.h"
#include <chrono>
#include <iomanip>
#include <iostream>
#include <vector>

namespace {
using namespace rapid_group_experiment;
using Clock = std::chrono::steady_clock;
struct Case { const char *name; size_t n, work; unsigned shape; };
constexpr Case cases[] = {
    {"loop64",64,0,0}, {"loop4096",4096,0,0},
    {"balanced64_100",64,100,0}, {"balanced64_1000",64,1000,0},
    {"tail64_100",64,100,1}, {"tail64_1000",64,1000,1},
    {"cluster64_1000",64,1000,2}, {"single64_1000",64,1000,3},
    {"balanced4096_100",4096,100,0}, {"tail4096_100",4096,100,1}};

size_t Units(const Case &c, size_t i) {
  if (c.shape == 1 && i >= c.n * 3 / 4) return c.work * 16;
  if (c.shape == 2 && ((i >= c.n / 8 && i < c.n / 4) ||
                      (i >= c.n * 5 / 8 && i < c.n * 3 / 4))) return c.work * 6;
  if (c.shape == 3 && i == c.n - 1) return c.work * 32;
  return c.work;
}
uint64_t Arithmetic(size_t i, size_t units) {
  uint64_t x = i + 1;
  asm volatile("" : "+r"(x));
  for (size_t j = 0; j < units; ++j) {
    x += UINT64_C(0x9e3779b97f4a7c15);
    x = (x ^ (x >> 30)) * UINT64_C(0xbf58476d1ce4e5b9);
    x = (x ^ (x >> 27)) * UINT64_C(0x94d049bb133111eb);
    x ^= x >> 31;
  }
  return x;
}
void Oracle(Runtime &runtime, size_t n) {
  auto visits = std::make_unique<std::atomic<unsigned>[]>(n);
  std::atomic<bool> outside{false};
  runtime.Run(0, n, [&](size_t i) {
    if (i >= n) { outside = true; return; }
    visits[i].fetch_add(1, std::memory_order_relaxed);
  });
  if (outside) throw std::runtime_error("out-of-range callback");
  for (size_t i = 0; i < n; ++i)
    if (visits[i] != 1) throw std::runtime_error("visitation oracle failed at " + std::to_string(i));
}
void Diagnose(Runtime &runtime) {
  const auto &o = runtime.Config();
  auto counts = std::make_unique<std::atomic<size_t>[]>(o.workers);
  auto visits = std::make_unique<std::atomic<unsigned>[]>(4096);
  std::vector<size_t> captured;
  std::atomic<bool> outside{false};
  for (unsigned r = 0; r < 5; ++r) {
    runtime.Prepare();
    captured.push_back(runtime.Run(0, 4096, [&](size_t i) {
      const size_t worker = runtime.Executor().CurrentThreadId();
      if (i >= 4096 || worker >= o.workers) { outside = true; return; }
      auto value = Arithmetic(i, i >= 3072 ? 1600 : 100);
      asm volatile("" : "+r"(value) : : "memory");
      ++visits[i]; ++counts[worker];
    }));
  }
  if (outside) throw std::runtime_error("diagnostic bounds failed");
  for (size_t i = 0; i < 4096; ++i)
    if (visits[i] != 5) throw std::runtime_error("diagnostic visitation oracle failed");
  const auto counters = runtime.Executor().Snapshot();
  std::cout << ",\"captured_helpers\":[";
  for (size_t i = 0; i < captured.size(); ++i) std::cout << (i ? "," : "") << captured[i];
  std::cout << "],\"callbacks_by_worker\":[";
  for (unsigned i = 0; i < o.workers; ++i) std::cout << (i ? "," : "") << counts[i].load();
  std::cout << "],\"group_publications\":" << counters.group_publications
            << ",\"routed\":" << counters.routed << ",\"routed_outside\":" << counters.routed_outside;
}
void Measure(Runtime &runtime) {
  std::cout << ",\"cases\":[";
  bool first = true;
  for (const auto &c : cases) {
    Oracle(runtime, c.n);
    const auto run = [&] {
      runtime.Run(0, c.n, [&](size_t i) {
        auto value = Arithmetic(i, Units(c, i));
        asm volatile("" : "+r"(value) : : "memory");
      });
    };
    for (unsigned i = 0; i < 64; ++i) run();
    const auto start = Clock::now();
    size_t calls = 0;
    double elapsed;
    do {
      for (unsigned i = 0; i < 16; ++i) run();
      calls += 16;
      elapsed = std::chrono::duration<double, std::nano>(Clock::now() - start).count();
    } while (elapsed < runtime.Config().minimum_ms * 1e6);
    std::cout << (first ? "" : ",") << "{\"case\":\"" << c.name << "\",\"n\":" << c.n
              << ",\"work\":" << c.work << ",\"pattern\":" << c.shape
              << ",\"calls\":" << calls << ",\"ns\":" << elapsed / calls << '}';
    first = false;
  }
  std::cout << ']';
}
} // namespace

int main(int argc, char **argv) {
  try {
    auto o = Parse(argc, argv);
#ifdef OOX_GROUP_DIAGNOSTICS
    if (!o.diagnose) throw std::invalid_argument("diagnostic binary requires --diagnose; never use it for timings");
#else
    if (o.diagnose) throw std::invalid_argument("use rapid_group_diagnose --diagnose");
#endif
    if (Arithmetic(0, 1) != UINT64_C(0x910a2dec89025cc1) ||
        Arithmetic(1, 2) != UINT64_C(0x64684c4f0fd784b4) ||
        Arithmetic(7, 100) != UINT64_C(0x15baf02e43887e81))
      throw std::runtime_error("arithmetic reference vectors failed");
    Runtime runtime(o);
    std::cout << std::setprecision(12)
              << "{\"schema\":\"rapid-group-experiment-v1\",\"workers\":" << o.workers
              << ",\"group_limit\":" << o.group << ",\"resident_limit\":" << o.residents
              << ",\"policy\":\"" << Name(o.policy) << "\",\"placement\":\"" << Name(o.placement)
              << "\",\"kernel\":\"arithmetic_v1\",\"grain\":2,\"instrumented\":" << (o.diagnose ? "true" : "false");
    if (o.diagnose) Diagnose(runtime); else Measure(runtime);
    std::cout << ",\"oracle_ok\":true}\n";
  } catch (const std::exception &e) { std::cerr << e.what() << '\n'; return 1; }
}
