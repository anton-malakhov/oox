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
#include <condition_variable>
#include <cstdint>
#include <exception>
#include <future>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>

namespace {

struct account {
    int points = 1000;
};

using namespace std::chrono_literals;

constexpr auto k_async_test_timeout = 2s;
constexpr auto k_registration_rendezvous_timeout = 500ms;

// Forces two lazy shared_var values to be materialized concurrently. Each
// constructor runs while setup() holds the mutex for its first shared_var, so
// opposite argument orders deterministically expose non-atomic registration.
// The bounded wait also lets the test pass after a fix that locks all states in
// a canonical order before materializing either value.
class registration_rendezvous {
    std::mutex mutex_;
    std::condition_variable cv_;
    bool enabled_ = false;
    int participants_ = 0;

public:
    void enable() {
        std::lock_guard<std::mutex> lock(mutex_);
        enabled_ = true;
        participants_ = 0;
    }

    void disable() {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            enabled_ = false;
        }
        cv_.notify_all();
    }

    void arrive() {
        thread_local bool already_arrived = false;
        std::unique_lock<std::mutex> lock(mutex_);
        if (!enabled_ || already_arrived) {
            return;
        }

        already_arrived = true;
        ++participants_;
        cv_.notify_all();
        cv_.wait_for(lock, k_registration_rendezvous_timeout,
                     [this] { return participants_ >= 2 || !enabled_; });
    }
};

registration_rendezvous writer_registration_rendezvous;

struct gated_value {
    int value = 0;

    gated_value() {
        writer_registration_rendezvous.arrive();
    }
};

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
            oox::run([](int& v) { v = 41; }, value);
            published.store(true, std::memory_order_release);
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

TEST(SharedVar, ConcurrentOppositeOrderMultiVarWritersComplete) {
    const auto completion = completes_within([] {
        writer_registration_rendezvous.enable();
        oox::shared_var<gated_value> first;
        oox::shared_var<gated_value> second;
        std::barrier start(2);

        std::thread first_registration([&] {
            start.arrive_and_wait();
            oox::run([](gated_value& a, gated_value& b) {
                ++a.value;
                ++b.value;
            }, first, second);
        });

        std::thread second_registration([&] {
            start.arrive_and_wait();
            oox::run([](gated_value& b, gated_value& a) {
                ++b.value;
                ++a.value;
            }, second, first);
        });

        first_registration.join();
        second_registration.join();
        writer_registration_rendezvous.disable();

        if (first.get().value != 2 || second.get().value != 2) {
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
    const int v = seen.load(std::memory_order_relaxed);
    EXPECT_TRUE(v == 0 || v == 7) << "reader must observe a consistent slot value, got " << v;
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
