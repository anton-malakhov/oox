// SPDX-License-Identifier: Apache-2.0
#include <array>
#include <atomic>
#include <cstdio>
#include <cstdint>
#include <thread>
#include <utility>
#include <vector>
#include <gtest/gtest.h>

#ifndef OOX_EIGEN_THREAD_POOL_TESTING
#error "The epoch protocol tests require OOX_EIGEN_THREAD_POOL_TESTING"
#endif

namespace epoch_test {
constexpr unsigned seed = 0x1010e;
inline thread_local unsigned case_id = 0;
struct Hooks {
  inline static std::atomic<unsigned> stage{0}, paused{0}, reader{0}, reader_paused{0};
  inline static std::atomic<unsigned> shared{0}, released{0}, body_entries{0};
  inline static std::atomic<uint64_t> acquire_mask{0}, acquired{0}, rejected{0};
  inline static std::atomic<bool> resume{true}, resume_acquired{true}, resume_reader{true}, resume_body{true};
  static void Release() {
    resume = resume_acquired = resume_reader = resume_body = true;
    stage = reader = 0;
    acquire_mask = 0;
  }
  static void Reset() {
    Release(); paused = reader_paused = shared = released = body_entries = 0;
    acquired = rejected = 0;
  }
  static void Pause(unsigned point) {
    if (point == 2) ++shared;
    unsigned expected = point;
    if (!stage.compare_exchange_strong(expected, 0)) return;
    paused = point;
    while (!resume.load()) std::this_thread::yield();
  }
  static void Acquired(unsigned worker) {
    if (worker >= 64) return;
    const uint64_t bit = uint64_t{1} << worker;
    if (!(acquire_mask.fetch_and(~bit) & bit)) return;
    acquired.fetch_or(bit);
    while (!resume_acquired.load()) std::this_thread::yield();
  }
  static void Reader(unsigned worker, uint64_t) {
    unsigned expected = worker;
    if (!worker || !reader.compare_exchange_strong(expected, 0)) return;
    reader_paused = worker;
    while (!resume_reader.load()) std::this_thread::yield();
  }
};
}
#define OOX_EIGEN_TEST_RESIDENT_BATCH_EDITING(word) epoch_test::Hooks::Pause(1)
#define OOX_EIGEN_TEST_RESIDENT_BATCH_MASK_READY(word) epoch_test::Hooks::Pause(2)
#define OOX_EIGEN_TEST_RESIDENT_BATCH_READER(worker, generation) epoch_test::Hooks::Reader(worker, generation)
#define OOX_EIGEN_TEST_RESIDENT_BATCH_REJECTED(worker) \
  do { if ((worker) < 64) epoch_test::Hooks::rejected.fetch_or(uint64_t{1} << (worker)); } while (false)
#define OOX_EIGEN_TEST_RESIDENT_BATCH_RELEASED(word) ++epoch_test::Hooks::released
#define OOX_EIGEN_TEST_RESIDENT_ACQUIRED(worker) epoch_test::Hooks::Acquired(worker)
#include <oox/eigen/rapid_start.h>
#include "test_support/eigen_resident.h"
#include "eigen_test_wait.h"

namespace {
using namespace oox::detail::eigen_pool;
using namespace oox::detail::eigen_pool::rapid;
using namespace epoch_test;
using namespace std::chrono_literals;
template <typename Predicate> void Wait(Predicate ready, const char *stage) {
  char context[160];
  std::snprintf(context, sizeof(context), "seed=%u case=%u %s", seed, case_id, stage);
  eigen_test_support::WaitUntil(ready, context);
}
struct Runtime {
  explicit Runtime(unsigned workers) : pool(workers, true, true, WorkerIdleMode::ResidentBusy), state(pool) {
    Hooks::Reset();
    eigen_test_support::WaitForResidentWorkers({&state, {0, workers}}, 5s);
  }
  ~Runtime() { Hooks::Release(); } // Release gates before the pool joins workers.
  ThreadPool pool;
  RapidDomainState state;
};
struct Command final : ResidentTask {
  explicit Command(ThreadPool &p, bool held = false) : pool(p), held(held) {}
  void Run(size_t slot) noexcept override {
    if (slot >= visits.size()) { outside = true; ++finished; return; }
    ++visits[slot];
    if (held) {
      ++Hooks::body_entries;
      while (!Hooks::resume_body.load()) std::this_thread::yield();
    }
    while (pool.TryExecuteSomething()) {}
    finished.fetch_add(1, std::memory_order_release);
  }
  void Check(unsigned second = 1) const {
    EXPECT_FALSE(outside.load());
    EXPECT_EQ(visits[0].load(), 0u);
    EXPECT_EQ(visits[1].load(), 1u);
    EXPECT_EQ(visits[2].load(), second);
  }
  ThreadPool &pool;
  bool held;
  std::array<std::atomic<unsigned>, 3> visits{};
  std::atomic<bool> outside{false};
  std::atomic<unsigned> finished{0};
};
std::array<unsigned, 2> Claim(Runtime &runtime, DomainId domain) {
  eigen_test_support::WaitForResidentWorkers({&runtime.state, domain}, 5s);
  std::array<unsigned, 2> workers{};
  const size_t count = runtime.pool.ClaimResidentWorkers(domain, workers.data(), 2);
  if (count != 2) { ADD_FAILURE() << "seed=" << seed << " case=" << case_id; eigen_test_support::FailWait("claim two helpers"); }
  return workers;
}
struct Invocation {
  Invocation(Runtime &runtime, DomainId domain, Command &command)
      : workers(Claim(runtime, domain)), cohort(runtime.pool, workers.data(), 2, remaining), command(command) {}
  ~Invocation() {
    if (!finalized) { Hooks::Release(); if (!published) Publish(); Join(); }
  }
  void Publish() { published = true; cohort.Publish(command); }
  bool Complete() const { return cohort.IsComplete(); }
  void Join() {
    Wait([&] { return Complete(); }, "captured command join");
    EXPECT_EQ(command.finished.load(std::memory_order_acquire), 2u) << "premature cohort completion";
    Wait([&] { return command.finished.load(std::memory_order_acquire) >= 2; }, "independent callback join");
    cohort.Finalize(); finalized = true;
    EXPECT_TRUE(cohort.IsComplete()) << "finalized cohort must remain ready";
  }
  std::array<unsigned, 2> workers;
  std::atomic<size_t> remaining{2};
  ResidentCohort cohort;
  Command &command;
  bool published = false, finalized = false;
};
struct Publisher {
  template <typename F> explicit Publisher(F &&function) : thread(std::forward<F>(function)) {}
  ~Publisher() { Hooks::Release(); Join(); }
  void Join() { if (thread.joinable()) thread.join(); }
  std::thread thread;
};
struct OrdinaryDrain {
  explicit OrdinaryDrain(Runtime &r, unsigned count) : runtime(r), executed(count), discarded(count) {}
  ~OrdinaryDrain() { Hooks::Release(); runtime.pool.Cancel(); Join(); }
  struct Task final : oox::detail::eigen_pool::Task {
    Task(OrdinaryDrain &owner, unsigned index) : owner(owner), index(index) {}
    void Finish(bool run) noexcept {
      auto *terminal = &owner.terminal;
      ++(run ? owner.executed[index] : owner.discarded[index]);
      delete this;
      terminal->fetch_add(1, std::memory_order_release);
    }
    void operator()() override { Finish(true); }
    void Discard() noexcept override { Finish(false); }
    OrdinaryDrain &owner;
    unsigned index;
  };
  void Submit(unsigned index) {
    auto *task = new Task(*this, index);
    ++submitted;
    runtime.pool.RunOnThread(task, 0);
  }
  void Join() { Wait([&] { return terminal.load(std::memory_order_acquire) >= submitted; }, "ordinary terminal join"); }
  Runtime &runtime;
  std::vector<std::atomic<unsigned>> executed, discarded;
  std::atomic<unsigned> terminal{0};
  unsigned submitted = 0;
};

TEST(EigenRapidEpoch, StaleNonmemberValidatesBeforeReadingReusedMetadata) {
  for (unsigned test = 0; test < 8; ++test) {
    case_id = 100 + test; SCOPED_TRACE(case_id);
    Runtime runtime(4);
    Hooks::reader = 3; Hooks::resume_reader = false;
    Command first(runtime.pool), second(runtime.pool);
    Invocation old(runtime, {1, 3}, first);
    old.Publish();
    Wait([&] { return Hooks::reader_paused == 3 && old.Complete(); }, "stale nonmember snapshot");
    old.Join();
    Invocation next(runtime, {2, 4}, second);
    next.Publish();
    Hooks::resume_reader = true;
    next.Join();
    EXPECT_NE(Hooks::rejected.load() & (uint64_t{1} << 3), 0u);
    first.Check(); second.Check();
  }
}

TEST(EigenRapidEpoch, DelayedOldJoinSurvivesFourNewerCommandsOnSameWorkers) {
  for (unsigned test = 0; test < 8; ++test) {
    case_id = 200 + test; SCOPED_TRACE(case_id);
    Runtime runtime(3);
    Command first(runtime.pool);
    Invocation old(runtime, {1, 3}, first);
    old.Publish();
    for (unsigned newer = 0; newer < 4; ++newer) {
      Command command(runtime.pool);
      Invocation next(runtime, {1, 3}, command);
      next.Publish(); next.Join(); command.Check();
    }
    EXPECT_TRUE(old.Complete());
    old.Join(); first.Check();
  }
}

TEST(EigenRapidEpoch, PendingPublicationAndSaturatedOrdinaryQueueHaveIndependentJoins) {
  constexpr unsigned capacity = 1024, count = 4098;
  for (unsigned phase : {1u, 2u}) for (bool cancel : {false, true}) for (unsigned test = 0; test < 2; ++test) {
    case_id = 300 + phase * 10 + unsigned(cancel) * 2 + test; SCOPED_TRACE(case_id);
    Runtime runtime(3);
    OrdinaryDrain ordinary(runtime, count); // Outlives every queued/acquired task.
    Hooks::acquire_mask = 6; Hooks::resume_acquired = false;
    ordinary.Submit(0); ordinary.Submit(1);
    Wait([] { return Hooks::acquired == 6; }, "ordinary acquisition before capture");
    Command command(runtime.pool);
    Invocation invocation(runtime, {1, 3}, command);
    Hooks::stage = phase; Hooks::resume = false;
    Publisher publisher([&] { invocation.Publish(); });
    Wait([&] { return Hooks::paused == phase; }, "publication paused before commit");
    for (unsigned i = 2; i < count; ++i) ordinary.Submit(i);
    for (unsigned i = 0; i < count; ++i) {
      EXPECT_EQ(ordinary.executed[i].load(), unsigned(i >= capacity + 2)) << i;
      EXPECT_EQ(ordinary.discarded[i].load(), 0u) << i;
    }
    if (cancel) runtime.pool.Cancel();
    Hooks::resume_acquired = Hooks::resume = true;
    publisher.Join(); invocation.Join(); ordinary.Join(); command.Check();
    EXPECT_EQ(ordinary.terminal.load(), count);
    for (unsigned i = 0; i < count; ++i) {
      const bool run = !cancel || i >= capacity + 2;
      EXPECT_EQ(ordinary.executed[i].load(), unsigned(run)) << i;
      EXPECT_EQ(ordinary.discarded[i].load(), unsigned(!run)) << i;
    }
  }
}

TEST(EigenRapidEpoch, PackedSaturationAndOldLegacyFinalizePreserveNewDescriptor) {
  if (!internal::resident_packed_clock_bits) GTEST_SKIP() << "tiny counters target";
  for (unsigned test = 0; test < 4; ++test) {
    case_id = 400 + test; SCOPED_TRACE(case_id);
    Runtime runtime(5);
    for (uint64_t version = 1; version <= internal::resident_packed_clock_max; ++version) {
      Command single(runtime.pool);
      std::atomic<size_t> remaining{1};
      unsigned worker = 0;
      eigen_test_support::WaitForResidentWorkers({&runtime.state, {1, 2}}, 5s);
      if (runtime.pool.ClaimResidentWorkers({1, 2}, &worker, 1) != 1) eigen_test_support::FailWait("single clock advance");
      runtime.pool.PublishResident(single, remaining, worker, 1);
      Wait([&] { return remaining.load(std::memory_order_acquire) == 0; }, "single clock completion");
      single.Check(0);
      const uint64_t expected = version < internal::resident_packed_clock_max ? version : 0;
      EXPECT_EQ(runtime.pool.NextResidentEpoch(), expected);
      runtime.pool.SetResidentLimit(0);
      Wait([&] { return runtime.pool.HasWaitingHelper(); }, "park while preserving packed bits");
      runtime.pool.SetResidentLimit(5);
      eigen_test_support::WaitForResidentWorkers({&runtime.state, {0, 5}}, 5s);
      EXPECT_EQ(runtime.pool.NextResidentEpoch(), expected);
    }
    Command first(runtime.pool), held(runtime.pool, true), later(runtime.pool);
    Invocation old(runtime, {1, 3}, first);
    const unsigned released = Hooks::released;
    old.Publish();
    Wait([&] { return old.Complete() && Hooks::released > released; }, "legacy helper descriptor release");
    Hooks::resume_body = false;
    Invocation active(runtime, {1, 3}, held);
    active.Publish();
    Wait([] { return Hooks::body_entries == 2; }, "new legacy callbacks held");
    old.Join(); // This old caller must not clear the active helpers' descriptor.
    Invocation direct(runtime, {3, 5}, later);
    const unsigned shared = Hooks::shared;
    direct.Publish();
    EXPECT_EQ(Hooks::shared.load(), shared);
    direct.Join(); Hooks::resume_body = true; active.Join();
    first.Check(); held.Check(); later.Check();
    EXPECT_EQ(runtime.pool.NextResidentEpoch(), 0u);
  }
}

TEST(EigenRapidEpoch, GlobalSaturationNeverReusesAnOldFinishedId) {
  if (internal::resident_global_clock_max > 3) GTEST_SKIP() << "tiny counters target";
  for (unsigned test = 0; test < 4; ++test) {
    case_id = 500 + test; SCOPED_TRACE(case_id);
    Runtime runtime(9);
    const auto run = [&] {
      Command command(runtime.pool); Invocation invocation(runtime, {1, 3}, command);
      invocation.Publish(); invocation.Join(); command.Check();
    };
    run(); // Epoch 1.
    Command first(runtime.pool), held(runtime.pool, true);
    Invocation old(runtime, {1, 3}, first); // Last valid epoch 2.
    old.Publish();
    Hooks::resume_body = false;
    Invocation active(runtime, {1, 3}, held); // Maximum permanently selects counters.
    active.Publish();
    Wait([] { return Hooks::body_entries == 2; }, "held global legacy callbacks");
    EXPECT_FALSE(active.Complete());
    EXPECT_EQ(runtime.pool.NextResidentEpoch(), 0u);
    Hooks::resume_body = true; active.Join(); held.Check();
    for (unsigned newer = 0; newer < 4; ++newer) run();
    EXPECT_TRUE(old.Complete()); old.Join(); first.Check();
    EXPECT_EQ(runtime.pool.NextResidentEpoch(), 0u);
  }
}

TEST(EigenRapidEpoch, BroadcastGenerationFreezesThenAlwaysPublishesDirect) {
  if (internal::resident_batch_generation_max > 6) GTEST_SKIP() << "tiny counters target";
  for (unsigned test = 0; test < 4; ++test) {
    case_id = 600 + test; SCOPED_TRACE(case_id);
    Runtime runtime(5);
    for (unsigned turn = 0; turn < 8; ++turn) {
      Command command(runtime.pool);
      auto workers = Claim(runtime, {1, 3});
      std::atomic<size_t> remaining{2};
      const unsigned released = Hooks::released;
      const size_t word = runtime.pool.PublishResidentBatch(command, remaining, workers.data(), 2);
      EXPECT_EQ(word != size_t(-1), turn < 3);
      Wait([&] { return remaining.load(std::memory_order_acquire) == 0; }, "bounded broadcast completion");
      if (turn < 3) Wait([&] { return Hooks::released > released; }, "bounded broadcast helper release");
      command.Check();
    }
    EXPECT_EQ(Hooks::shared.load(), 3u);
  }
}
TEST(EigenRapidEpoch, FinalizedCompletionDoesNotCompleteTheNextHeldGeneration) {
  for (unsigned workers : {3u, 8u}) for (unsigned test = 0; test < 16; ++test) {
    case_id = 700 + workers * 16 + test; SCOPED_TRACE(case_id);
    Runtime runtime(workers);
    Command first(runtime.pool), held(runtime.pool, true);
    Invocation old(runtime, {1, 3}, first);
    old.Publish(); old.Join(); first.Check();
    Hooks::resume_body = false;
    Invocation next(runtime, {1, 3}, held);
    next.Publish();
    Wait([] { return Hooks::body_entries == 2; }, "next generation callbacks held");
    EXPECT_FALSE(next.Complete()) << "old completion bits completed a new generation";
    EXPECT_TRUE(old.Complete()) << "new publication invalidated an already finalized cohort";
    Hooks::resume_body = true;
    next.Join(); held.Check();
  }
}

TEST(EigenRapidEpoch, ManualPositiveEpochBatchRetainsFinishedRecordContract) {
  for (unsigned workers : {3u, 8u}) for (unsigned test = 0; test < 16; ++test) {
    case_id = 900 + workers * 16 + test; SCOPED_TRACE(case_id);
    Runtime runtime(workers);
    auto captured = Claim(runtime, {1, 3});
    const uint64_t epoch = runtime.pool.NextResidentEpoch();
    EXPECT_NE(epoch, 0u) << "the first fresh-pool claim must have a positive epoch";
    Command command(runtime.pool);
    std::atomic<size_t> remaining{2};
    const size_t word = runtime.pool.PublishResidentBatch(
        command, remaining, captured.data(), 2, 1, epoch);
    EXPECT_NE(word, size_t(-1));
    Wait([&] {
      return epoch ? runtime.pool.ResidentEpochComplete(epoch, captured.data(), 2)
                   : remaining.load(std::memory_order_acquire) == 0;
    }, "default manual batch completion");
    EXPECT_EQ(command.finished.load(std::memory_order_acquire), 2u);
    command.Check();
    if (epoch) {
      EXPECT_EQ(remaining.load(), 2u) << "manual epoch completion must not consume the counter";
      runtime.pool.ReleaseResidentEpochBatch(word);
    }
  }
}
} // namespace

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
