// Copyright (C) 2026
//
// SPDX-License-Identifier: Apache-2.0

// Unit tests for oox::shared_var<T> — the thread-safe, copyable counterpart
// of oox::var<T>. See docs/design-shared-var.md.

#include <oox/shared_var.h>
#include <gtest/gtest.h>

#include <atomic>
#include <thread>
#include <vector>

namespace {

struct account {
    int points = 1000;
};

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
