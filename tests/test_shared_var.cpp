// Copyright (C) 2026
//
// SPDX-License-Identifier: Apache-2.0

// Unit tests for oox::shared_var<T> — the thread-safe, copyable counterpart
// of oox::var<T>. See docs/design-shared-var.md.

#include <oox/shared_var.h>
#include <gtest/gtest.h>

#include <atomic>
#include <barrier>
#include <chrono>
#include <cstdint>
#include <exception>
#include <future>
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>

namespace {

struct account {
    int points = 1000;
};

struct non_assignable_value {
    non_assignable_value() = default;
    non_assignable_value(const non_assignable_value&) = default;
    non_assignable_value(non_assignable_value&&) = default;
    non_assignable_value& operator=(const non_assignable_value&) = delete;
    non_assignable_value& operator=(non_assignable_value&&) = delete;
};

struct copy_only_value {
    int value = 0;

    copy_only_value() = default;
    explicit copy_only_value(int v) : value(v) {}
    copy_only_value(const copy_only_value&) = default;
    copy_only_value(copy_only_value&&) = delete;
    copy_only_value& operator=(const copy_only_value&) = default;
    copy_only_value& operator=(copy_only_value&&) = delete;
};

struct copy_only_value_initialized {
    int value;

    copy_only_value_initialized() = default;
    copy_only_value_initialized(const copy_only_value_initialized&) noexcept = default;
    copy_only_value_initialized(copy_only_value_initialized&&) = delete;
};

struct throwing_move_copyable_value {
    int value = 0;

    throwing_move_copyable_value() noexcept = default;
    throwing_move_copyable_value(const throwing_move_copyable_value&) noexcept = default;
    throwing_move_copyable_value(throwing_move_copyable_value&&) {
        throw std::runtime_error("move construction failed");
    }
};

struct asymmetric_assignment_value {
    int value = 0;

    asymmetric_assignment_value() = default;
    explicit asymmetric_assignment_value(int v) : value(v) {}
    asymmetric_assignment_value(const asymmetric_assignment_value&) = default;
    asymmetric_assignment_value(asymmetric_assignment_value&&) = default;
    asymmetric_assignment_value& operator=(const asymmetric_assignment_value& other) noexcept {
        value = other.value;
        return *this;
    }
    asymmetric_assignment_value& operator=(asymmetric_assignment_value&) {
        throw std::runtime_error("non-const assignment selected");
    }
};

struct throwing_assignment_value {
    int value = 0;

    throwing_assignment_value() = default;
    explicit throwing_assignment_value(int v) : value(v) {}
    throwing_assignment_value(const throwing_assignment_value&) = default;
    throwing_assignment_value(throwing_assignment_value&&) = default;
    throwing_assignment_value& operator=(const throwing_assignment_value&) {
        throw std::runtime_error("copy assignment failed");
    }
    throwing_assignment_value& operator=(throwing_assignment_value&&) {
        throw std::runtime_error("move assignment failed");
    }
};

struct throwing_default_value {
    throwing_default_value() {
        throw std::runtime_error("default construction failed");
    }
    throwing_default_value(const throwing_default_value&) noexcept = default;
    throwing_default_value(throwing_default_value&&) noexcept = default;
};

struct throwing_copy_value {
    throwing_copy_value() noexcept = default;
    throwing_copy_value(const throwing_copy_value&) {
        throw std::runtime_error("copy construction failed");
    }
    throwing_copy_value(throwing_copy_value&&) noexcept = default;
};

struct throwing_int_conversion {
    throwing_int_conversion(int) {
        throw std::runtime_error("cross-type conversion failed");
    }
};

struct immovable_default_value {
    immovable_default_value() = default;
    immovable_default_value(const immovable_default_value&) = delete;
    immovable_default_value(immovable_default_value&&) = delete;
};

template <typename Var, typename Value>
concept supports_value_assignment = requires(Var& var, Value&& value) {
    var = std::forward<Value>(value);
};

static_assert(!supports_value_assignment<oox::var<non_assignable_value>, non_assignable_value>);
static_assert(!supports_value_assignment<oox::shared_var<non_assignable_value>, non_assignable_value>);
static_assert(!supports_value_assignment<oox::var<throwing_assignment_value, false>,
                                         throwing_assignment_value>);
static_assert(!supports_value_assignment<oox::shared_var<throwing_assignment_value, false>,
                                         throwing_assignment_value>);
static_assert(!oox::internal::shareable_value<oox::shared_var<int>>);
static_assert(!oox::internal::policy_value_materializable<throwing_default_value, false>);
static_assert(oox::internal::policy_value_materializable<throwing_default_value, true>);
static_assert(!oox::internal::value_materializable<immovable_default_value>);
static_assert(!oox::internal::argument_conversions_are_nothrow<
              oox::internal::types<throwing_copy_value>,
              oox::internal::types<throwing_copy_value&>>::value);
static_assert(oox::internal::argument_conversions_are_nothrow<
              oox::internal::types<throwing_copy_value>,
              oox::internal::types<throwing_copy_value&&>>::value);
#if OOX_EXCEPTIONS_ENABLED
static_assert(supports_value_assignment<oox::var<throwing_assignment_value, true>,
                                        throwing_assignment_value>);
static_assert(supports_value_assignment<oox::shared_var<throwing_assignment_value, true>,
                                        throwing_assignment_value>);
#endif

std::atomic<std::uint64_t> default_constructor_count{0};
oox::shared_var<int>* plain_constructor_side_effect = nullptr;

struct counted_default_value {
    int value = 0;

    counted_default_value() noexcept {
        default_constructor_count.fetch_add(1, std::memory_order_relaxed);
    }
};

#if defined(OOX_TEST_INJECT_TASK_SPAWN_FAILURE) && OOX_TEST_INJECT_TASK_SPAWN_FAILURE
std::atomic<bool>* publication_fallback_started = nullptr;
std::atomic<bool>* publication_fallback_release = nullptr;

struct publication_fallback_value {
    int value = 42;

    publication_fallback_value() noexcept {
        publication_fallback_started->store(true, std::memory_order_release);
        while (!publication_fallback_release->load(std::memory_order_acquire)) {
            std::this_thread::yield();
        }
    }
};
#endif

struct reentrant_plain_default_value {
    reentrant_plain_default_value() noexcept {
        oox::run([](int& side_effect) { ++side_effect; }, *plain_constructor_side_effect);
    }
};

using namespace std::chrono_literals;

constexpr auto k_async_test_timeout = 2s;
#if OOX_EXCEPTIONS_ENABLED
struct shared_var_test_error final : std::exception {};
#endif

template <typename F>
testing::AssertionResult completes_within(F&& scenario, const char* timeout_message) {
    auto completion = std::make_shared<std::promise<void>>();
    auto completion_future = completion->get_future();
    std::thread worker([scenario = std::forward<F>(scenario), completion]() mutable {
        try {
            scenario();
            completion->set_value();
        } catch (...) {
            completion->set_exception(std::current_exception());
        }
    });

    if (completion_future.wait_for(k_async_test_timeout) != std::future_status::ready) {
        // The scenario owns all of its state. Detaching it lets this test report
        // a bounded failure even when the implementation has deadlocked.
        worker.detach();
        return testing::AssertionFailure() << timeout_message;
    }

    worker.join();
    try {
        completion_future.get();
    } catch (const std::exception& e) {
        return testing::AssertionFailure() << "scenario threw: " << e.what();
    } catch (...) {
        return testing::AssertionFailure() << "scenario threw a non-standard exception";
    }
    return testing::AssertionSuccess();
}

void drain_backend_task_epilogues() {
#if defined(OOX_USING_TF)
    oox::internal::get_tf_pool().wait_for_all();
#endif
}

testing::AssertionResult life_count_reaches(oox::internal::task_node* task, int expected) {
    const auto deadline = std::chrono::steady_clock::now() + k_async_test_timeout;
    int actual;
    do {
        actual = task->life_get_count();
        if (actual == expected) {
            return testing::AssertionSuccess();
        }
        std::this_thread::yield();
    } while (std::chrono::steady_clock::now() < deadline);
    return testing::AssertionFailure()
        << "task life count remained " << actual << ", expected " << expected;
}

} // namespace

/////////////////////////////////////// BASIC API ////////////////////////////////////////

TEST(SharedVar, ReadyValue) {
    oox::shared_var<int> sv(42);
    EXPECT_EQ(sv.get(), 42);
    EXPECT_EQ(oox::wait_and_get(sv), 42);
}

TEST(SharedVar, LazyDefault) {
    oox::shared_var<int> sv;
    EXPECT_EQ(sv.get(), 0); // lazy var materializes a default value
}

TEST(SharedVar, LazyMaterializationUsesValueInitializationAndSafeCopy) {
    oox::shared_var<copy_only_value_initialized, false> initialized;
    auto initialized_done = oox::run<false>([](copy_only_value_initialized& value) noexcept {
        ++value.value;
    }, initialized);
    oox::wait_for_all(initialized_done);
    EXPECT_EQ(initialized.get().value, 1);

    oox::shared_var<throwing_move_copyable_value, false> copied;
    auto copied_done = oox::run<false>([](throwing_move_copyable_value& value) noexcept {
        value.value = 42;
    }, copied);
    oox::wait_for_all(copied_done);
    EXPECT_EQ(copied.get().value, 42);
}

TEST(SharedVar, LazyMaterializationRunsUserCodeInGraphTasks) {
    const auto completion = completes_within([] {
        const auto constructors_before =
            default_constructor_count.load(std::memory_order_relaxed);
        oox::shared_var<counted_default_value> first;
        oox::shared_var<counted_default_value> second;

        std::thread left([&] {
            oox::run([](counted_default_value& a, counted_default_value& b) {
                ++a.value;
                ++b.value;
            }, first, second);
        });
        std::thread right([&] {
            oox::run([](counted_default_value& b, counted_default_value& a) {
                ++b.value;
                ++a.value;
            }, second, first);
        });
        left.join();
        right.join();
        const int first_value = first.get().value;
        const int second_value = second.get().value;
        const auto constructors_after =
            default_constructor_count.load(std::memory_order_relaxed);
        if (first_value != 2 || second_value != 2
            || constructors_after != constructors_before + 2) {
            throw std::runtime_error("lazy materialization lost a writer or constructor side effect");
        }
    }, "lazy materializer graph tasks did not complete");
    EXPECT_TRUE(completion);
}

TEST(SharedVar, ConcurrentGetPublishesOneLazyMaterializer) {
    constexpr int kThreads = 16;
    const auto constructors_before =
        default_constructor_count.load(std::memory_order_relaxed);
    oox::shared_var<counted_default_value> value;
    std::barrier start(kThreads);
    std::atomic<int> completed{0};
    std::vector<std::thread> getters;
    getters.reserve(kThreads);

    for (int i = 0; i < kThreads; ++i) {
        getters.emplace_back([&] {
            start.arrive_and_wait();
            if (value.get().value == 0) {
                completed.fetch_add(1, std::memory_order_relaxed);
            }
        });
    }
    for (auto& getter : getters) {
        getter.join();
    }

    EXPECT_EQ(completed.load(std::memory_order_relaxed), kThreads);
    EXPECT_EQ(default_constructor_count.load(std::memory_order_relaxed),
              constructors_before + 1);
}

#if defined(OOX_TEST_INJECT_TASK_SPAWN_FAILURE) && OOX_TEST_INJECT_TASK_SPAWN_FAILURE
TEST(SharedVar, ConsecutiveSpawnFailuresCompleteMaterializerAndWaiterInline) {
    std::atomic<bool> fallback_started{false};
    std::atomic<bool> release_fallback{false};
    std::atomic<bool> waiter_subscribed{false};
    std::atomic<unsigned> spawn_failures{0};
    publication_fallback_started = &fallback_started;
    publication_fallback_release = &release_fallback;
    oox::shared_var<publication_fallback_value> value;

    oox::internal::observe_shared_var_waiter_subscription(waiter_subscribed);
    oox::internal::inject_task_spawn_failures(spawn_failures, 2);
    auto publisher = std::async(std::launch::async, [&] {
        try {
            (void)value.get();
        } catch (const oox::internal::injected_task_spawn_failure&) {
            return true;
        }
        return false;
    });

    const auto deadline = std::chrono::steady_clock::now() + k_async_test_timeout;
    while (!fallback_started.load(std::memory_order_acquire)
           && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::yield();
    }
    ASSERT_TRUE(fallback_started.load(std::memory_order_acquire));

    auto waiter = std::async(std::launch::async, [&] {
        return value.get().value;
    });
    while (!waiter_subscribed.load(std::memory_order_acquire)) {
        std::this_thread::yield();
    }
    EXPECT_EQ(waiter.wait_for(20ms), std::future_status::timeout);

    release_fallback.store(true, std::memory_order_release);
    EXPECT_TRUE(publisher.get());
    ASSERT_EQ(waiter.wait_for(k_async_test_timeout), std::future_status::ready);
    EXPECT_EQ(waiter.get(), 42);
    EXPECT_EQ(value.get().value, 42);
    EXPECT_EQ(spawn_failures.load(std::memory_order_acquire), 0u);
    oox::internal::clear_task_spawn_failure_injection();
    publication_fallback_started = nullptr;
    publication_fallback_release = nullptr;
}
#endif

TEST(SharedVar, ReentrantPlainVarSetupUsesAnIndependentRegistrationBatch) {
    oox::shared_var<int> value(0);
    oox::shared_var<int> side_effect(0);
    oox::var<reentrant_plain_default_value> plain;
    plain_constructor_side_effect = &side_effect;
    auto done = oox::run([](int& target, reentrant_plain_default_value&) {
        ++target;
    }, value, plain);
    oox::wait_for_all(done);
    EXPECT_EQ(value.get(), 1);
    EXPECT_EQ(side_effect.get(), 1);
    plain_constructor_side_effect = nullptr;
}

TEST(SharedVar, MoveValue) {
    oox::shared_var<std::string> sv(std::string("hello"));
    EXPECT_EQ(sv.get(), "hello");
}

TEST(SharedVar, DeferredPublication) {
    oox::shared_var<int> sv(oox::deferred);
    auto result = oox::run([](int& v) { v = 41; }, sv);
    oox::wait_for_all(result);
    EXPECT_EQ(sv.get(), 41);
}

TEST(SharedVar, AdoptVar) {
    auto v = oox::run([] { return 7; });
    oox::shared_var<int> sv(std::move(v));
    EXPECT_EQ(sv.get(), 7);
}

TEST(SharedVar, AdoptForwardedVar) {
    constexpr std::uint64_t expected = 0x1122334455667788ULL;
    auto forwarded = oox::run([] {
        return oox::run([] { return expected; });
    });

    oox::shared_var<std::uint64_t> sv(std::move(forwarded));
    EXPECT_EQ(sv.get(), expected);
}

TEST(SharedVar, RegisterAdoptedForwardedVarBeforeProducerRuns) {
    oox::shared_var<int> gate(oox::deferred);
    auto forwarded = oox::run([](int input) {
        return oox::run([input] { return input + 1; });
    }, gate);
    oox::shared_var<int> value(std::move(forwarded));
    auto result = oox::run([](int input) { return input + 1; }, value);
    gate = 40;
    EXPECT_EQ(oox::wait_and_get(result), 42);
}

TEST(SharedVar, CopyHandlesShareState) {
    oox::shared_var<int> sv(10);
    oox::shared_var<int> copy = sv;
    EXPECT_EQ(copy.get(), 10);
    copy = 20; // write through the shared state
    EXPECT_EQ(sv.get(), 20);
}

TEST(SharedVar, MoveHandle) {
    oox::shared_var<int> sv(1);
    oox::shared_var<int> moved = std::move(sv);
    EXPECT_EQ(moved.get(), 1);
}

TEST(SharedVar, AssignmentSerializes) {
    oox::shared_var<int> sv;
    sv = 5;
    EXPECT_EQ(sv.get(), 5);
    sv = 7;
    EXPECT_EQ(sv.get(), 7);
}

TEST(SharedVar, CopyOnlyAssignmentUsesTheCopyOverload) {
    copy_only_value initial(1);
    copy_only_value replacement(42);
    oox::shared_var<copy_only_value> value(initial);
    value = replacement;
    EXPECT_EQ(value.get().value, 42);
}

TEST(SharedVar, CopyAssignmentUsesTheConstQualifiedExpression) {
    asymmetric_assignment_value initial(1);
    const asymmetric_assignment_value replacement(42);
    oox::shared_var<asymmetric_assignment_value, false> value(initial);
    value = replacement;
    EXPECT_EQ(value.get().value, 42);
}

TEST(SharedVar, AssignmentPublishesAfterReleasingStateLock) {
    const auto completion = completes_within([] {
        oox::shared_var<int> value(oox::deferred);
        oox::shared_var<int> alias = value;
        auto reader = oox::run([alias](int) { return alias.get(); }, value);
        value = 42;
        if (oox::wait_and_get(reader) != 42) {
            throw std::runtime_error("reader observed the wrong assigned value");
        }
    }, "assignment published a task while retaining the shared state lock");
    EXPECT_TRUE(completion);
}

TEST(SharedVar, DeferredAssignmentWakesWaitingReader) {
#if defined(OOX_USING_SERIAL)
    GTEST_SKIP() << "the serial backend does not implement blocking waits";
#else
    oox::shared_var<int> value(oox::deferred);
    std::promise<void> reader_started;
    std::atomic<int> observed{-1};
    std::thread reader([&] {
        reader_started.set_value();
        observed.store(value.get(), std::memory_order_relaxed);
    });
    reader_started.get_future().wait();
    std::this_thread::sleep_for(20ms);
    value = 42;
    reader.join();
    EXPECT_EQ(observed.load(std::memory_order_relaxed), 42);
#endif
}

TEST(SharedVar, FailedWaiterSubscriptionReleasesArc) {
    oox::var<int> ready(1);
    auto* waiter = oox::internal::task::allocate<oox::internal::shared_var_waiter>();
    waiter->life_set_count(1);
    EXPECT_FALSE(waiter->subscribe(ready.current_task));
    waiter->release();
}

/////////////////////////////////////// GRAPH INTEGRATION ////////////////////////////////////////

TEST(SharedVar, WriterChain) {
    oox::shared_var<int> value(0);
    oox::run([](int& v) { v = 1; }, value);
    oox::run([](int& v) { v = 2; }, value);
    oox::run([](int& v) { v = 3; }, value);
    EXPECT_EQ(oox::wait_and_get(value), 3);
}

TEST(SharedVar, ReaderSeesValueBeforeWriter) {
    oox::shared_var<int> value(1);
    auto doubled = oox::run([](int v) { return v * 2; }, value);
    oox::run([](int& v) { v = 100; }, value);
    EXPECT_EQ(oox::wait_and_get(doubled), 2);
    EXPECT_EQ(value.get(), 100);
}

TEST(SharedVar, ManyReadersOneWriter) {
    oox::shared_var<int> value(3);
    auto r1 = oox::run([](const int& v) { return v + 1; }, value);
    auto r2 = oox::run([](int v) { return v + 2; }, value);
    oox::run([](int& v) { v = 10; }, value);
    EXPECT_EQ(oox::wait_and_get(r1), 4);
    EXPECT_EQ(oox::wait_and_get(r2), 5);
    EXPECT_EQ(oox::wait_and_get(value), 10);
}

TEST(SharedVar, DeferredReaderAndWriter) {
    oox::shared_var<int> source(oox::deferred);
    oox::shared_var<int> sink;
    sink = oox::run([](int v) { return v + 1; }, source); // reader on deferred
    oox::run([](int& v) { v = 41; }, source);             // writer publishes
    EXPECT_EQ(oox::wait_and_get(sink), 42);
}

TEST(SharedVar, SameVarTwiceAsReader) {
    oox::shared_var<int> value(10);
    auto sum = oox::run([](int a, int b) { return a + b; }, value, value);
    EXPECT_EQ(oox::wait_and_get(sum), 20);
}

TEST(SharedVar, SameStateTwiceAsWriter) {
    oox::shared_var<int> value(0);
    auto done = oox::run([](int& a, int& b) {
        ++a;
        ++b;
    }, value, value);
    oox::wait_for_all(done);
    drain_backend_task_epilogues();
    EXPECT_TRUE(life_count_reaches(done.current_task, 2));
    EXPECT_EQ(value.get(), 2);
}

TEST(SharedVar, CopiedStateTwiceAsWriter) {
    oox::shared_var<int> value(0);
    oox::shared_var<int> alias = value;
    auto done = oox::run([](int& a, int& b) {
        ++a;
        ++b;
    }, value, alias);
    oox::wait_for_all(done);
    drain_backend_task_epilogues();
    EXPECT_TRUE(life_count_reaches(done.current_task, 2));
    EXPECT_EQ(value.get(), 2);
    EXPECT_EQ(alias.get(), 2);
}

TEST(SharedVar, SameStateAsReaderAndWriterUsesOneRegistration) {
    oox::shared_var<int> value(3);
    auto done = oox::run([](int& target, int snapshot) {
        target += snapshot;
    }, value, value);
    oox::wait_for_all(done);
    EXPECT_EQ(value.get(), 6);
}

TEST(SharedVar, DeferredMixedAliasesMaterializeBeforeEveryArgument) {
    oox::shared_var<int> read_first(oox::deferred);
    auto first = oox::run([](int old, int& out) noexcept { out = old + 1; },
                          read_first, read_first);
    oox::wait_for_all(first);
    EXPECT_EQ(read_first.get(), 1);

    oox::shared_var<int> write_first(oox::deferred);
    auto second = oox::run([](int& out, int old) noexcept { out = old + 1; },
                           write_first, write_first);
    oox::wait_for_all(second);
    EXPECT_EQ(write_first.get(), 1);
}

#if OOX_EXCEPTIONS_ENABLED
TEST(SharedVar, BareLazyThrowingMaterializationIsAsynchronousAndReleasesTask) {
    std::weak_ptr<int> captured;
    auto scenario = [&] {
        oox::shared_var<throwing_default_value, true> value;
        auto resource = std::make_shared<int>(1);
        captured = resource;
        auto done = oox::run<true>([resource](throwing_default_value&) noexcept {}, value);
        resource.reset();
        EXPECT_THROW(oox::wait_for_all(done), std::runtime_error);
        drain_backend_task_epilogues();
    };
    EXPECT_NO_THROW(scenario());
    EXPECT_TRUE(captured.expired());
}

TEST(SharedVar, ThrowingDeferredMaterializationPropagates) {
    oox::shared_var<throwing_default_value, true> value(oox::deferred);
    auto done = oox::run<true>([](throwing_default_value&) noexcept {}, value);

    EXPECT_THROW(oox::wait_for_all(done), std::runtime_error);
    EXPECT_THROW((void)value.get(), oox::cancelled_by_exception);
}

TEST(SharedVar, CrossTypeConversionBecomesGraphFailure) {
    oox::shared_var<int, true> value(1);
    auto done = oox::run<true>([](throwing_int_conversion) noexcept {}, value);
    EXPECT_THROW(oox::wait_for_all(done), std::runtime_error);
}

TEST(SharedVar, NonThrowingWriterUsesSafeCopyForThrowingStatePolicy) {
    oox::shared_var<throwing_move_copyable_value, true> value;
    auto done = oox::run<false>([](throwing_move_copyable_value& target) noexcept {
        target.value = 42;
    }, value);
    EXPECT_NO_THROW(oox::wait_for_all(done));
    EXPECT_EQ(value.get().value, 42);
}

TEST(SharedVar, ThrowingPolicyCopyAssignmentUsesTheConstQualifiedExpression) {
    asymmetric_assignment_value initial(1);
    const asymmetric_assignment_value replacement(42);
    oox::shared_var<asymmetric_assignment_value, true> value(initial);
    value = replacement;
    EXPECT_EQ(value.get().value, 42);
}

TEST(SharedVar, ThrowingActualCopyConversionPropagates) {
    oox::shared_var<throwing_copy_value, true> value(throwing_copy_value{});
    auto done = oox::run<true>([](throwing_copy_value) noexcept {}, value);

    EXPECT_THROW(oox::wait_for_all(done), std::runtime_error);
}

TEST(SharedVar, ExceptionWakesWaiterAndRethrows) {
#if defined(OOX_USING_SERIAL)
    GTEST_SKIP() << "the serial backend does not implement blocking waits";
#else
    oox::shared_var<int, true> gate(oox::deferred);
    auto producer = oox::run<true>([](int) -> int { throw shared_var_test_error{}; }, gate);
    oox::shared_var<int, true> value(std::move(producer));
    auto result = std::async(std::launch::async, [&] { return value.get(); });
    std::this_thread::sleep_for(20ms);
    gate = 1;
    EXPECT_THROW((void)result.get(), shared_var_test_error);
#endif
}

TEST(SharedVar, ForwardedProducerExceptionRethrows) {
    oox::shared_var<int, true> gate(oox::deferred);
    auto producer = oox::run<true>([](int) -> oox::var<int, true> {
        throw shared_var_test_error{};
    }, gate);
    oox::shared_var<int, true> value(std::move(producer));
    std::atomic<bool> dependent_ran{false};
    auto dependent = oox::run<true>([&](int input) {
        dependent_ran.store(true, std::memory_order_relaxed);
        return input;
    }, value);
    gate = 1;
    EXPECT_THROW((void)value.get(), shared_var_test_error);
    EXPECT_THROW((void)oox::wait_and_get(dependent), shared_var_test_error);
    EXPECT_FALSE(dependent_ran.load(std::memory_order_relaxed));
}

TEST(SharedVar, ForwardedProducerFailureCanBeRecovered) {
    auto failed_writer = oox::run<true>([]() -> oox::var<int, true> {
        throw shared_var_test_error{};
    });
    oox::shared_var<int, true> written(std::move(failed_writer));
    auto recovery = oox::run<true>([](int& value) { value = 42; }, written);
    oox::wait_for_all(recovery);
    EXPECT_EQ(written.get(), 42);

    auto failed_assignment = oox::run<true>([]() -> oox::var<int, true> {
        throw shared_var_test_error{};
    });
    oox::shared_var<int, true> assigned(std::move(failed_assignment));
    assigned = 43;
    EXPECT_EQ(assigned.get(), 43);
}

TEST(SharedVar, ThrowingValueAssignmentPropagates) {
    oox::shared_var<throwing_assignment_value, true> copied(throwing_assignment_value{1});
    const throwing_assignment_value replacement(2);
    copied = replacement;
    EXPECT_THROW((void)copied.get(), oox::cancelled_by_exception);

    oox::shared_var<throwing_assignment_value, true> moved(throwing_assignment_value{1});
    moved = throwing_assignment_value{2};
    EXPECT_THROW((void)moved.get(), oox::cancelled_by_exception);
}

TEST(SharedVar, CancellationPropagatesThroughWaiter) {
    oox::shared_var<int, true> gate(oox::deferred);
    auto producer = oox::run<true>([](int input) { return input + 1; }, gate);
    oox::shared_var<int, true> value(std::move(producer));
    value.cancel();
#if defined(OOX_USING_SERIAL)
    gate = 1;
    EXPECT_THROW((void)value.get(), oox::cancelled_by_user);
#else
    auto result = std::async(std::launch::async, [&] { return value.get(); });
    std::this_thread::sleep_for(20ms);
    gate = 1;
    EXPECT_THROW((void)result.get(), oox::cancelled_by_user);
#endif
}
#endif

TEST(SharedVar, MixedVarAndSharedVar) {
    oox::var<int> plain(5);
    oox::shared_var<int> shared(6);
    auto sum = oox::run([](int a, int b) { return a + b; }, plain, shared);
    EXPECT_EQ(oox::wait_and_get(sum), 11);
}

TEST(SharedVar, MixedVarAndSharedVarWriters) {
    oox::var<int> plain(0);
    oox::shared_var<int> shared(0);
    auto done = oox::run([](int& p, int& s) { p = 1; s = 2; }, plain, shared);
    oox::wait_for_all(done);
    EXPECT_EQ(oox::wait_and_get(plain), 1);
    EXPECT_EQ(oox::wait_and_get(shared), 2);
}

TEST(SharedVar, VoidResultTask) {
    oox::shared_var<int> value(0);
    auto done = oox::run([](int& v) { v += 5; }, value);
    oox::wait_for_all(done);
    EXPECT_EQ(value.get(), 5);
}

/////////////////////////////////////// MULTI-THREADED ////////////////////////////////////////

TEST(SharedVar, ConcurrentGet) {
    oox::shared_var<int> value(42);
    constexpr int N = 8;
    std::atomic<int> ok{0};
    std::vector<std::thread> threads;
    for (int i = 0; i < N; ++i) {
        threads.emplace_back([&] {
            if (value.get() == 42) {
                ok.fetch_add(1, std::memory_order_relaxed);
            }
        });
    }
    for (auto& t : threads) {
        t.join();
    }
    EXPECT_EQ(ok.load(std::memory_order_relaxed), N);
}

TEST(SharedVar, ConcurrentWriterRegistration) {
    oox::shared_var<int> value(0);
    constexpr int N = 8;
    std::vector<std::thread> threads;
    for (int i = 0; i < N; ++i) {
        threads.emplace_back([&, i] {
            oox::run([i](int& v) { v += i; }, value); // result dropped immediately
        });
    }
    for (auto& t : threads) {
        t.join();
    }
    // Writers serialize on the state mutex: all N increments must be applied.
    EXPECT_EQ(oox::wait_and_get(value), N * (N - 1) / 2);
}

TEST(SharedVar, WaitOnDeferredDoesNotBlockPublication) {
#if defined(OOX_USING_SERIAL)
    GTEST_SKIP() << "the serial backend does not implement blocking waits";
#else
    const auto completion = completes_within([] {
        oox::shared_var<int> value(oox::deferred);
        std::atomic<bool> published{false};
        std::promise<void> publisher_started;
        auto publisher_started_future = publisher_started.get_future();

        std::thread publisher([&] {
            publisher_started.set_value();
            std::this_thread::sleep_for(250ms);
            oox::run([&published](int& v) {
                v = 41;
                // Set inside the writer: on backends where run() blocks until
                // the task completes, a flag set after run() returns would be
                // observed later than wait()'s unblock, failing the oracle.
                published.store(true, std::memory_order_release);
            }, value);
        });

        publisher_started_future.wait();
        value.wait();
        if (!published.load(std::memory_order_acquire)) {
            throw std::runtime_error("wait() returned before deferred publication");
        }
        publisher.join();
        if (value.get() != 41) {
            throw std::runtime_error("deferred publication produced the wrong value");
        }
    }, "wait() retained the shared_var mutex and blocked publication");
    EXPECT_TRUE(completion);
#endif
}

TEST(SharedVar, ForwardedWaitDoesNotBlockWriterRegistration) {
#if defined(OOX_USING_SERIAL)
    GTEST_SKIP() << "the serial backend does not implement blocking waits";
#else
    const auto completion = completes_within([] {
        oox::shared_var<int> gate(oox::deferred);
        auto forwarded = oox::run([](int input) {
            return oox::run([input] { return input; });
        }, gate);
        oox::shared_var<int> value(std::move(forwarded));
        std::promise<void> getter_started;
        auto getter = std::async(std::launch::async, [&] {
            getter_started.set_value();
            return value.get();
        });
        getter_started.get_future().wait();
        std::this_thread::sleep_for(20ms);
        auto registration = std::async(std::launch::async, [&] {
            oox::run([](int& input) { ++input; }, value);
        });
        const bool registered_before_publication =
            registration.wait_for(100ms) == std::future_status::ready;
        gate = 41;
        registration.get();
        const int observed = getter.get();
        if (!registered_before_publication || observed != 42) {
            throw std::runtime_error("forwarded wait retained the state mutex");
        }
    }, "forwarded get() blocked a writer registration on the state mutex");
    EXPECT_TRUE(completion);
#endif
}

TEST(SharedVar, ConcurrentOppositeOrderMultiVarWritersComplete) {
    const auto completion = completes_within([] {
        oox::shared_var<int> first(0);
        oox::shared_var<int> second(0);
        std::barrier start(2);

        std::thread first_registration([&] {
            start.arrive_and_wait();
            oox::run([](int& a, int& b) {
                ++a;
                ++b;
            }, first, second);
        });

        std::thread second_registration([&] {
            start.arrive_and_wait();
            oox::run([](int& b, int& a) {
                ++b;
                ++a;
            }, second, first);
        });

        first_registration.join();
        second_registration.join();
        if (first.get() != 2 || second.get() != 2) {
            throw std::runtime_error("one of the two writer tasks did not run");
        }
    }, "opposite per-variable registration orders created a task cycle");
    EXPECT_TRUE(completion);
}

TEST(SharedVar, ConcurrentCopyAndUse) {
    oox::shared_var<int> value(5);
    std::atomic<int> got{0};
    std::vector<std::thread> threads;
    for (int i = 0; i < 8; ++i) {
        threads.emplace_back([&] {
            oox::shared_var<int> copy = value; // copy the handle in another thread
            got.fetch_add(copy.get(), std::memory_order_relaxed);
        });
    }
    for (auto& t : threads) {
        t.join();
    }
    EXPECT_EQ(got.load(std::memory_order_relaxed), 8 * 5);
}

TEST(SharedVar, WriterSwitchWhileGetPending) {
    // A get() snapshots a slot that a concurrent registration may switch away
    // from; the retained slot reference must prevent a use-after-free.
    oox::shared_var<int> value(0);
    std::atomic<int> seen{-1};
    std::thread reader([&] {
        seen.store(value.get(), std::memory_order_relaxed);
    });
    std::thread writer([&] {
        oox::run([](int& v) { v = 7; }, value);
    });
    reader.join();
    writer.join();
    value.wait();
    const int v = seen.load(std::memory_order_relaxed);
    EXPECT_TRUE(v == 0 || v == 7) << "reader must observe a consistent slot value, got " << v;
}

TEST(SharedVar, GetWhileFastWriters) {
    // Regression: a reader's get() racing a fast writer chain used to wait on
    // a task freed at its own completion (the chain consumes the slot hold),
    // crashing with task::wait(): Assertion life_get_count(). The wait now
    // happens without the state mutex and re-validates the current slot, so
    // this must not crash on any backend. (The window is only reachable on
    // async backends — TBB/twist — where the next writer is chained before
    // the current task completes; on std/serial the registration blocks and
    // the test passes trivially.)
    oox::shared_var<std::uint32_t> value(0);
    constexpr int kRegistrations = 4000;
    constexpr int kGets = 1000;
    std::atomic<int> registrations{0};
    std::thread writer([&] {
        int i = 0;
        while (registrations.load(std::memory_order_relaxed) < kRegistrations) {
            oox::run([i](std::uint32_t& v) {
                std::uint32_t acc = static_cast<std::uint32_t>(i);
                for (int j = 0; j < 20000; ++j) {
                    acc = acc * 31 + static_cast<std::uint32_t>(j);
                }
                v = acc;
            }, value);
            registrations.fetch_add(1, std::memory_order_relaxed);
            ++i;
        }
    });
    volatile std::uint32_t sink = 0;
    for (int i = 0; i < kGets; ++i) {
        sink = value.get(); // must not crash
    }
    registrations.store(kRegistrations, std::memory_order_relaxed);
    writer.join();
    (void)value.get(); // the chain drains; read the final value
    (void)sink;
}

int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    int err{0};
#if OOX_EXCEPTIONS_ENABLED
    try {
        err = RUN_ALL_TESTS();
    } catch (const std::exception& e) {
        fprintf(stderr, "Error: %s\n", e.what());
    }
#else
    err = RUN_ALL_TESTS();
#endif
    return err;
}
