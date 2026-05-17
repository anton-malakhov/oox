// Copyright (C) 2021 Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

bool g_oox_verbose = false;
#define oox_println(s, ...) fprintf(stderr, s "\n",  __VA_ARGS__)
#define __OOX_TRACE if (g_oox_verbose) oox_println
#define __OOX_ASSERT(b, m) if(!(b)) { oox_println("OOX assertion failed: " #b " at line %d: %s", __LINE__, m); abort(); }
#define __OOX_ASSERT_EX(b, m) __OOX_ASSERT(b,m)

#define REMARK oox_println
#define ASSERT(b, m) ASSERT_TRUE(b) << (m)

#include <oox/oox.h>
#include <gtest/gtest.h>

#include <iostream>
#include <numeric>
#include <vector>
#include <functional>
#if OOX_CONTEXTS_ENABLED
#include <future>
#include <stdexcept>
#include <atomic>
#include <condition_variable>
#include <mutex>
#endif


/////////////////////////////////////// EXAMPLES ////////////////////////////////////////

#include "../examples/fibonacci.h"
#include "../examples/filesystem.h"
#include "../examples/wavefront.h"

namespace ArchSample {
    // example from original OOX
    using T = int;
    T f() { return 1; }
    T g() { return 2; }
    T h() { return 3; }
    T test() {
        oox::var<T> a, b, c;
        oox::run([](T&A){A=f();}, a);
        oox::run([](T&B){B=g();}, b);
        oox::run([](T&C){C=h();}, c);
        oox::run([](T&A, T B){A+=B;}, a, b);
        oox::run([](T&B, T C){B+=C;}, b, c);
        oox::wait_for_all(b);
        return oox::wait_and_get(a);
    }
}

#if OOX_SERIAL_DEBUG
#define OOX OOX_SQ
#elif HAVE_TBB
#define OOX OOX_TBB
#elif HAVE_TF
#define OOX OOX_TF
#elif HAVE_FOLLY
#define OOX OOX_FOLLY
#else
#define OOX OOX_STD
#endif

auto plus = std::plus<int>();

#if OOX_CONTEXTS_ENABLED
namespace {

class task_gate {
public:
    void notify_started() {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            started_ = true;
        }
        cv_.notify_all();
    }

    void wait_until_started() {
        std::unique_lock<std::mutex> lock(mutex_);
        cv_.wait(lock, [&] { return started_; });
    }

    void release() {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            released_ = true;
        }
        cv_.notify_all();
    }

    void wait_until_released() {
        std::unique_lock<std::mutex> lock(mutex_);
        cv_.wait(lock, [&] { return released_; });
    }

private:
    std::mutex mutex_;
    std::condition_variable cv_;
    bool started_ = false;
    bool released_ = false;
};

template <typename Body>
struct launched_task {
    std::future<oox::var<int>> future;
    std::thread thread;
};

template <typename Body>
launched_task<Body> launch_blocked_task(oox::ctx::context& ctx, std::atomic<int>& runs, task_gate& gate, Body&& body) {
    std::promise<oox::var<int>> promise;
    auto future = promise.get_future();
    std::thread thread([&ctx, &runs, &gate, body = std::forward<Body>(body), promise = std::move(promise)]() mutable {
        promise.set_value(oox::run(ctx, [&]() -> int {
            ++runs;
            gate.notify_started();
            gate.wait_until_released();
            return body();
        }));
    });
    return {std::move(future), std::move(thread)};
}

template <typename Callable>
void expect_runtime_error(Callable&& callable, const char* message) {
    try {
        if constexpr (std::is_void_v<std::invoke_result_t<Callable>>) {
            callable();
        } else {
            [[maybe_unused]] auto ignored = callable();
        }
        FAIL() << "expected std::runtime_error";
    } catch (const std::runtime_error& e) {
        EXPECT_STREQ(e.what(), message);
    } catch (...) {
        FAIL() << "expected std::runtime_error";
    }
}

template <typename Callable>
void expect_task_cancelled(Callable&& callable) {
    EXPECT_THROW(
        [&] {
            if constexpr (std::is_void_v<std::invoke_result_t<Callable>>) {
                callable();
            } else {
                [[maybe_unused]] auto ignored = callable();
            }
        }(),
        oox::task_cancelled);
}

struct rethrow_if_failed_probe final : oox::internal::task_node_slots<1> {
    explicit rethrow_if_failed_probe(oox::ctx::context* ctx_ptr = nullptr) : ctx(ctx_ptr) {}
    oox::ctx::context* get_ctx() const noexcept override { return ctx; }
    oox::ctx::context* ctx;
};

} // namespace
#endif

TEST(OOX, Simple) {
    const oox::var<int> a = oox::run(plus, 2, 3);
    oox::var<int> b = oox::run(plus, 1, a);
    ASSERT_EQ(oox::wait_and_get(a), 5);
    ASSERT_EQ(b.get(), 6);
}
TEST(OOX, Empty) {
    oox::var<int> a;
    oox::var<int> b = oox::run(plus, 1, a);
    oox::run([](int &A){ A = 2; }, a);
    ASSERT_EQ(oox::wait_and_get(a), 2);
    const auto b_value = oox::wait_and_get(b);
    ASSERT_TRUE(b_value == 1 || b_value == 3);
}
TEST(OOX, Arch) {
    int arch = ArchSample::test();
    ASSERT_EQ(arch, 3);
}
TEST(OOX, Fib) {
#ifdef OOX_USING_STD
    int x = 15;
#else
    int x = 5;
#endif
    using namespace Fibonacci;
    int fib0 = Serial::Fib(x);
    int fib1 = oox::wait_and_get(OOX1::Fib(x));
    int fib2 = oox::wait_and_get(OOX2::Fib(x));
    ASSERT_TRUE(fib0 == fib1 && fib1 == fib2);
}
TEST(OOX, FS) {
    int files0 = Filesystem::Serial::disk_usage(Filesystem::tree[0]);
    int files1 = oox::wait_and_get(Filesystem::Simple::disk_usage(Filesystem::tree[0]));
    ASSERT_EQ(files0, files1);
#ifdef HAVE_TBB
    int files2 = oox::wait_and_get(Filesystem::AntiDependence::disk_usage(Filesystem::tree[0]));
    //int files3 = oox::wait_and_get(Filesystem::Extended::disk_usage(Filesystem::tree[0]));
    ASSERT_EQ(files0, files2);
    //ASSERT_EQ(files0, files3);
#endif
}
TEST(OOX, Wavefront) {
    using namespace Wavefront_LCS;
    int lcs0 = Serial::LCS(input1, sizeof(input1), input2, sizeof(input2));
    int lcs1 = Straight::LCS(input1, sizeof(input1), input2, sizeof(input2));
    ASSERT_EQ(lcs0, lcs1);
}

TEST(OOX, Consistency) {
    auto func = []() -> oox::var<int> {
        return oox::run(std::plus<int>(), 1, 1);
    };
    const auto res = oox::wait_and_get(oox::run(func));
    ASSERT_EQ(res, 2);
}

TEST(OOX, ConsistencyInfLoop) {
    const oox::var<int> tmp = 1;
    ASSERT_EQ(oox::wait_and_get(tmp), 1);
}


TEST(OOX, DeferredChain) {
    oox::var<int> a(oox::deferred);
    oox::var<int> b = oox::run(plus, 1, a);
    oox::var<int> c = oox::run(plus, 1, b);

    oox::run([](int &A){ A = 10; }, a);

    ASSERT_EQ(oox::wait_and_get(a), 10);
    ASSERT_EQ(oox::wait_and_get(b), 11);
    ASSERT_EQ(oox::wait_and_get(c), 12);
}

TEST(OOX, DeferredDiamond) {
    oox::var<int> a(oox::deferred);

    oox::var<int> b = oox::run(plus, 1, a);
    oox::var<int> c = oox::run(plus, 2, a);
    oox::var<int> d = oox::run([](int x, int y){ return x + y; }, b, c);

    oox::run([](int &A){ A = 5; }, a);

    ASSERT_EQ(oox::wait_and_get(a), 5);
    ASSERT_EQ(oox::wait_and_get(b), 6);
    ASSERT_EQ(oox::wait_and_get(c), 7);
    ASSERT_EQ(oox::wait_and_get(d), 13);
}

TEST(OOX, DeferredTwoInputs) {
    oox::var<int> a(oox::deferred);
    oox::var<int> b(oox::deferred);

    oox::var<int> c = oox::run([](int x, int y){ return x + y; }, a, b);

    oox::run([](int &B){ B = 3; }, b);
    oox::run([](int &A){ A = 2; }, a);

    ASSERT_EQ(oox::wait_and_get(a), 2);
    ASSERT_EQ(oox::wait_and_get(b), 3);
    ASSERT_EQ(oox::wait_and_get(c), 5);
}

TEST(OOX, DeferredChainWithMultipleWriters) {
    oox::var<int> a(oox::deferred);
    oox::var<int> b = oox::run(plus, 1, a);

    oox::run([](int &A){ A = 1; }, a);
    oox::run([](int &A){ A = 10; }, a);

    int aval = oox::wait_and_get(a);
    int bval = oox::wait_and_get(b);

    EXPECT_EQ(aval, 10);
    ASSERT_TRUE(bval == 2 || bval == 11);
}

TEST(OOX, DeferredForwardingLayer) {

    oox::var<int> a(oox::deferred);

    auto inner = [](oox::var<int> aa) -> oox::var<int> {
        return oox::run(plus, 1, aa);            // aa + 1
    };

    auto outer = [inner](oox::var<int> aa) -> oox::var<int> {
        // creates a forwarding task
        return oox::run(inner, aa);
    };

    oox::var<int> result = oox::run(outer, a);

    oox::run([](int &A){ A = 41; }, a);

    ASSERT_EQ(oox::wait_and_get(a), 41);
    ASSERT_EQ(oox::wait_and_get(result), 42);
}

TEST(OOX, DeferredArrayLayered) {

    oox::var<int> a[3] = {
        oox::var<int>(oox::deferred),
        oox::var<int>(oox::deferred),
        oox::var<int>(oox::deferred)
    };

    a[1] = oox::run(plus, 1, a[0]);
    a[2] = oox::run(plus, 1, a[1]);

    oox::run([](int &x){ x = 100; }, a[0]);

    ASSERT_EQ(oox::wait_and_get(a[0]), 100);
    ASSERT_EQ(oox::wait_and_get(a[1]), 101);
    ASSERT_EQ(oox::wait_and_get(a[2]), 102);
}

#if OOX_CONTEXTS_ENABLED
TEST(OOX, RethrowIfFailedReturnsWhenContextActive) {
    oox::ctx::context ctx;
    rethrow_if_failed_probe probe(&ctx);

    EXPECT_NO_THROW(probe.rethrow_if_failed());
}

TEST(OOX, RethrowIfFailedRethrowsStoredContextException) {
    oox::ctx::context ctx;
    rethrow_if_failed_probe probe(&ctx);
    constexpr const char* kMessage = "rethrow_if_failed exception";

    ctx.record_exception(std::make_exception_ptr(std::runtime_error(kMessage)));

    expect_runtime_error([&] { probe.rethrow_if_failed(); }, kMessage);
}

TEST(OOX, RethrowIfFailedThrowsCancelledForCancelledContext) {
    oox::ctx::context ctx;
    rethrow_if_failed_probe probe(&ctx);

    EXPECT_TRUE(ctx.cancel());
    expect_task_cancelled([&] { probe.rethrow_if_failed(); });
}

TEST(OOX, RethrowIfFailedThrowsCancelledForFailedHeadWithoutContext) {
    rethrow_if_failed_probe probe(nullptr);
    probe.head.store(reinterpret_cast<oox::internal::arc*>(oox::internal::k_task_failed_tag),
                     std::memory_order_release);

    expect_task_cancelled([&] { probe.rethrow_if_failed(); });
}

TEST(OOX, CancelledBeforeReadyTaskDoesNotSpawn) {
    oox::ctx::context ctx;
    oox::var<int> input(oox::deferred);
    std::atomic<int> runs{0};

    auto result = oox::run(ctx, [&](int value) {
        ++runs;
        return value + 1;
    }, input);

    ASSERT_TRUE(ctx.cancel());

    oox::run([](int& value) { value = 41; }, input);

    EXPECT_EQ(oox::wait_and_get(input), 41);
    EXPECT_EQ(runs.load(), 0);
    expect_task_cancelled([&] { return oox::wait_and_get(result); });
}

TEST(OOX, CancelledAfterStartTaskCompletesButReadIsPoisoned) {
    oox::ctx::context ctx;
    std::atomic<int> runs{0};
    task_gate gate;
    auto launched = launch_blocked_task(ctx, runs, gate, [] { return 7; });

    gate.wait_until_started();

    EXPECT_TRUE(ctx.cancel());

    gate.release();

    launched.thread.join();
    auto result = launched.future.get();

    EXPECT_EQ(runs.load(), 1);
    EXPECT_TRUE(ctx.is_cancelled());
    EXPECT_FALSE(ctx.has_exception());
    expect_task_cancelled([&] { return oox::wait_and_get(result); });
}

TEST(OOX, CancelledAfterStartThrownExceptionMarksContext) {
    oox::ctx::context ctx;
    std::atomic<int> runs{0};
    task_gate gate;
    constexpr const char* kMessage = "boom after cancel";

    auto launched = launch_blocked_task(ctx, runs, gate, []() -> int {
        throw std::runtime_error(kMessage);
    });

    gate.wait_until_started();
    EXPECT_TRUE(ctx.cancel());
    EXPECT_FALSE(ctx.has_exception());

    gate.release();

    launched.thread.join();
    auto result = launched.future.get();

    EXPECT_EQ(runs.load(), 1);
    expect_runtime_error([&] { return oox::wait_and_get(result); }, kMessage);
    EXPECT_TRUE(ctx.is_cancelled());
    EXPECT_TRUE(ctx.has_exception());
    expect_runtime_error([&] { ctx.rethrow_if_exception(); }, kMessage);
}

TEST(OOX, CancelledAfterStartThrownExceptionPropagatesToLateConsumer) {
    oox::ctx::context ctx;
    std::atomic<int> runs{0};
    task_gate gate;
    constexpr const char* kMessage = "late consumer sees original";

    auto launched = launch_blocked_task(ctx, runs, gate, []() -> int {
        throw std::runtime_error(kMessage);
    });

    gate.wait_until_started();
    EXPECT_TRUE(ctx.cancel());
    gate.release();

    launched.thread.join();
    auto failed = launched.future.get();

    expect_runtime_error([&] { return oox::wait_and_get(failed); }, kMessage);
    EXPECT_TRUE(ctx.has_exception());

    auto late = oox::run(ctx, [](int value) { return value + 1; }, failed);
    expect_runtime_error([&] { return oox::wait_and_get(late); }, kMessage);
}

TEST(OOX, ThrownTaskCancelledMarksContextAsException) {
    oox::ctx::context ctx;

    auto failed = oox::run(ctx, []() -> int {
        throw oox::task_cancelled();
    });

    expect_task_cancelled([&] { return oox::wait_and_get(failed); });
    EXPECT_TRUE(ctx.is_cancelled());
    EXPECT_TRUE(ctx.has_exception());
    expect_task_cancelled([&] { ctx.rethrow_if_exception(); });
}

TEST(OOX, ForwardedChainWithExplicitContextStaysValid) {
    oox::ctx::context ctx;

    auto result = oox::run(ctx, [&]() -> oox::var<int> {
        return oox::run(ctx, [&]() -> oox::var<int> {
            return oox::run(ctx, [] { return 7; });
        });
    });

    EXPECT_EQ(oox::wait_and_get(result), 7);
}

TEST(OOX, ExceptionPoisonsSiblingReadsInSameContext) {
    oox::ctx::context ctx;
    constexpr const char* kMessage = "ctx poisoning";

    auto ok = oox::run(ctx, [] { return 7; });
    auto failed = oox::run(ctx, []() -> int {
        throw std::runtime_error(kMessage);
    });

    expect_runtime_error([&] { return oox::wait_and_get(failed); }, kMessage);
    expect_runtime_error([&] { return oox::wait_and_get(ok); }, kMessage);
}

TEST(OOX, ContextResetClearsStoredException) {
    oox::ctx::context ctx;
    constexpr const char* kMessage = "reset clears exception";

    auto failed = oox::run(ctx, []() -> int {
        throw std::runtime_error(kMessage);
    });

    expect_runtime_error([&] { return oox::wait_and_get(failed); }, kMessage);
    EXPECT_TRUE(ctx.is_cancelled());
    EXPECT_TRUE(ctx.has_exception());
    expect_runtime_error([&] { ctx.rethrow_if_exception(); }, kMessage);

    ctx.reset();

    EXPECT_FALSE(ctx.is_cancelled());
    EXPECT_FALSE(ctx.has_exception());
    EXPECT_NO_THROW(ctx.rethrow_if_exception());
}
#endif

int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "-v") == 0)
            g_oox_verbose = true;
    }

    int err{0};
#if OOX_CONTEXTS_ENABLED
    try {
        err = RUN_ALL_TESTS();
    } catch (const std::exception& e) {
        REMARK("Error: %s", e.what());
    }
#else
    err = RUN_ALL_TESTS();
#endif
    return err;
}
