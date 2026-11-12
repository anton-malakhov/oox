// SPDX-License-Identifier: Apache-2.0
#include "experiment.h"
#include "tests/eigen_test_wait.h"
#include <iostream>
#include <vector>

namespace {
using namespace rapid_group_experiment;
constexpr uint64_t seed = 0x47524f5550313238;
void Check(bool condition, const std::string &message) {
  if (!condition) throw std::runtime_error(message);
}
void TargetOracle() {
  // Each source traverses exactly the selected destination set. Dense odd,
  // non-power-of-two sizes and 128/256 test indices beyond a single bit mask.
  for (unsigned p : {2u,3u,4u,5u,8u,17u,64u,65u,128u,256u})
    for (unsigned g = 1; g <= std::min(p, 64u); ++g)
      for (auto placement : {Placement::All, Placement::Outsiders}) {
        if (placement == Placement::Outsiders && p == g) continue;
        const unsigned first = placement == Placement::Outsiders ? g : 0;
        for (unsigned source = 0; source < g; ++source) {
          std::vector<unsigned> visits(p);
          for (unsigned i = 0; i < p - first; ++i) {
            const auto target = Target(placement, p, g, source, i);
            Check(target >= first && target < p, "target bounds");
            ++visits[target];
          }
          for (unsigned i = 0; i < p; ++i)
            Check(visits[i] == unsigned(i >= first), "target cycle oracle");
        }
      }
  std::vector<unsigned> initial(8);
  for (unsigned source = 0; source < 2; ++source)
    for (unsigned i = 0; i < 4; ++i) ++initial[Target(Placement::All, 8, 2, source, i)];
  for (unsigned count : initial) Check(count == 1, "publishers must start in different pool sectors");
}
void SerialOracle(Runtime &runtime) {
  uint64_t random = seed;
  for (unsigned test = 0; test < 40; ++test) {
    random = random * UINT64_C(6364136223846793005) + 1;
    const size_t n = test < 32 ? test : test < 38 ? 33 + random % 1024
        : test == 38 ? 65537 : 1048579;
    const size_t first = test % 2 ? size_t(-1) - n : random % 8192;
    const size_t grain = test >= 38 ? 17 : test % 9;
    auto actual = std::make_unique<std::atomic<unsigned>[]>(n);
    std::vector<unsigned> expected(n);
    for (size_t i = 0; i < n; ++i) expected[i] = unsigned(i % 31 + 1);
    std::atomic<bool> outside{false};
    runtime.Prepare();
    const auto captured = runtime.Run(first, first + n, [&](size_t i) {
      if (i < first || i - first >= n) { outside = true; return; }
      actual[i - first].fetch_add(unsigned((i - first) % 31 + 1), std::memory_order_relaxed);
    }, grain);
    const auto context = "seed=" + std::to_string(seed) + " case=" + std::to_string(test) +
        " p=" + std::to_string(runtime.Config().workers) + " g=" + std::to_string(runtime.Config().group) +
        " policy=" + std::string(Name(runtime.Config().policy)) + " placement=" + std::string(Name(runtime.Config().placement));
    Check(!outside && captured < runtime.Config().group, context + " bounds/capture");
    for (size_t i = 0; i < n; ++i)
      Check(actual[i] == expected[i], context + " index=" + std::to_string(i));
  }
}
void OutsiderProgress(Options o) {
  o.group = 2;
  Runtime runtime(o);
  std::atomic<unsigned> entered{0};
  std::atomic<bool> outside_work{false};
  auto visits = std::make_unique<std::atomic<unsigned>[]>(4096);
  const auto captured = runtime.Run(0, 4096, [&](size_t i) {
    const auto worker = runtime.Executor().CurrentThreadId();
    if (worker < 2 && (i == 0 || i == 2048)) {
      entered.fetch_or(1u << worker);
      eigen_test_support::WaitUntil([&] { return outside_work.load(); }, "nonmember helps both blocked initial owners");
    } else if (worker >= 2) {
      eigen_test_support::WaitUntil([&] { return entered.load() == 3; }, "both initial callbacks started");
      outside_work = true;
    }
    ++visits[i];
  }, 1);
  Check(captured == 1 && outside_work && entered == 3, "outsider progress proof failed");
  for (unsigned i = 0; i < 4096; ++i) Check(visits[i] == 1, "outsider visitation oracle");
  const auto counts = runtime.Executor().Snapshot();
  if (o.placement != Placement::Local)
    Check(counts.routed > 0 && counts.routed_outside > 0, "addressed publication did not reach outsiders");
}
void NestedAndException(Options o) {
  Runtime runtime(o);
  std::array<std::atomic<unsigned>, 7 * 9> visits{};
  runtime.Run(0, 7, [&](size_t i) {
    const auto nested = runtime.Run(0, 9, [&](size_t j) { ++visits[i * 9 + j]; }, 1);
    Check(nested == 0, "nested call must not capture another group");
  }, 1);
  for (const auto &value : visits) Check(value == 1, "nested serial oracle");
  bool caught = false;
  try {
    runtime.Run(0, 257, [](size_t i) { if (i == 13) throw std::runtime_error("expected"); }, 1);
  } catch (const std::runtime_error &e) { caught = std::string_view(e.what()) == "expected"; }
  Check(caught, "exception propagation");
  std::atomic<unsigned> total{0};
  runtime.Run(0, 257, [&](size_t) { ++total; }, 1);
  Check(total == 257, "reuse after exception");
}
} // namespace

int main(int argc, char **argv) {
  try {
    // Explicit --workers allows the same oracle on a large CPU. Default local
    // tests deliberately avoid oversubscribing laptops with 128 busy waiters.
    const auto base = Parse(argc, argv);
    Check(base.workers >= 4, "progress tests require at least four workers");
    TargetOracle();
    unsigned configurations = 0;
    for (auto policy : {Policy::Lazy, Policy::Auto, Policy::Simple, Policy::Static, Policy::Affinity}) {
      for (auto placement : {Placement::Local, Placement::All, Placement::Outsiders}) {
        if (placement != Placement::Local && policy != Policy::Auto && policy != Policy::Simple) continue;
        for (unsigned group : {2u, std::min(base.workers, 64u)}) {
          if (placement == Placement::Outsiders && group == base.workers) continue;
          Options o = base; o.policy = policy; o.placement = placement; o.group = group;
          { Runtime runtime(o); SerialOracle(runtime); }
          NestedAndException(o);
          ++configurations;
        }
        Options o = base; o.policy = policy; o.placement = placement;
        OutsiderProgress(o);
        o.residents = 2; OutsiderProgress(o);
      }
    }
    std::cout << "PASS seed=" << seed << " configurations=" << configurations
              << " generated_cases=" << configurations * 40
              << " nested/exception/nonmember-progress; destination oracle through 256 workers\n";
  } catch (const std::exception &e) { std::cerr << e.what() << '\n'; return 1; }
}
