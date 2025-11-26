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
#include <stdexcept>
#include <atomic>


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
        (void)oox::run([](T&A){A=f();}, a);
        (void)oox::run([](T&B){B=g();}, b);
        (void)oox::run([](T&C){C=h();}, c);
        (void)oox::run([](T&A, T B){A+=B;}, a, b);
        (void)oox::run([](T&B, T C){B+=C;}, b, c);
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

TEST(OOX, Simple) {
    const oox::var<int> a = oox::run(plus, 2, 3);
    oox::var<int> b = oox::run(plus, 1, a);
    ASSERT_EQ(oox::wait_and_get(a), 5);
    ASSERT_EQ(b.get(), 6);
}
TEST(OOX, Empty) {
    oox::var<int> a;
    oox::var<int> b = oox::run(plus, 1, a);
    (void)oox::run([](int &A){ A = 2; }, a);
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



/////////////////////////////////////// DEFERRED ////////////////////////////////////////

TEST(OOX, DeferredChain) {
    oox::var<int> a(oox::deferred);
    oox::var<int> b = oox::run(plus, 1, a);
    oox::var<int> c = oox::run(plus, 1, b);

    (void)oox::run([](int &A){ A = 10; }, a);

    ASSERT_EQ(oox::wait_and_get(a), 10);
    ASSERT_EQ(oox::wait_and_get(b), 11);
    ASSERT_EQ(oox::wait_and_get(c), 12);
}

TEST(OOX, DeferredDiamond) {
    oox::var<int> a(oox::deferred);

    oox::var<int> b = oox::run(plus, 1, a);
    oox::var<int> c = oox::run(plus, 2, a);
    oox::var<int> d = oox::run([](int x, int y){ return x + y; }, b, c);

    (void)oox::run([](int &A){ A = 5; }, a);

    ASSERT_EQ(oox::wait_and_get(a), 5);
    ASSERT_EQ(oox::wait_and_get(b), 6);
    ASSERT_EQ(oox::wait_and_get(c), 7);
    ASSERT_EQ(oox::wait_and_get(d), 13);
}

TEST(OOX, DeferredTwoInputs) {
    oox::var<int> a(oox::deferred);
    oox::var<int> b(oox::deferred);

    oox::var<int> c = oox::run([](int x, int y){ return x + y; }, a, b);

    (void)oox::run([](int &B){ B = 3; }, b);
    (void)oox::run([](int &A){ A = 2; }, a);

    ASSERT_EQ(oox::wait_and_get(a), 2);
    ASSERT_EQ(oox::wait_and_get(b), 3);
    ASSERT_EQ(oox::wait_and_get(c), 5);
}

TEST(OOX, DeferredChainWithMultipleWriters) {
    oox::var<int> a(oox::deferred);
    oox::var<int> b = oox::run(plus, 1, a);

    (void)oox::run([](int &A){ A = 1; }, a);
    (void)oox::run([](int &A){ A = 10; }, a);

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

    (void)oox::run([](int &A){ A = 41; }, a);

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

    (void)oox::run([](int &x){ x = 100; }, a[0]);

    ASSERT_EQ(oox::wait_and_get(a[0]), 100);
    ASSERT_EQ(oox::wait_and_get(a[1]), 101);
    ASSERT_EQ(oox::wait_and_get(a[2]), 102);
}

/////////////////////////////////////// EXCEPTIONS ////////////////////////////////////////

struct dummy_exception final : std::exception {
    [[nodiscard]] const char* what() const noexcept override { return "dummy throw"; }
};

TEST(OOX, ExceptionReturnRethrowsOriginal) {
    oox::var<int> a = oox::run([]() -> int {
        throw dummy_exception{};
    });
    EXPECT_THROW((void)oox::wait_and_get(a), dummy_exception);
}

TEST(OOX, ExceptionPropagatesThroughChainAndSkipsUserCode) {
    oox::var<int> a = oox::run([]() -> int {
        throw dummy_exception{};
    });

    std::atomic<int> ran_b{0};
    std::atomic<int> ran_c{0};

    oox::var<int> b = oox::run([&](int x) -> int {
        ran_b.fetch_add(1);
        return x + 1;
    }, a);

    oox::var<int> c = oox::run([&](int x) -> int {
        ran_c.fetch_add(1);
        return x + 1;
    }, b);

    EXPECT_THROW((void)oox::wait_and_get(c), dummy_exception);
    EXPECT_EQ(ran_b.load(), 0);
    EXPECT_EQ(ran_c.load(), 0);
}

TEST(OOX, ExceptionWaitForAllRethrows) {
    oox::node n = oox::run([]() {
        throw dummy_exception{};
    });

    EXPECT_THROW(oox::wait_for_all(n), dummy_exception);
}

TEST(OOX, ExceptionLateConsumerAfterProducerCompleted) {
    oox::var<int> a = oox::run([]() -> int {
        throw dummy_exception{};
    });

    EXPECT_THROW(oox::wait_for_all(a), dummy_exception);

    oox::var<int> b = oox::run(plus, 1, a);
    EXPECT_THROW((void)oox::wait_and_get(b), dummy_exception);
}

TEST(OOX, ExceptionDeferredWriterPoison) {
    oox::var<int> a(oox::deferred);
    oox::var<int> b = oox::run(plus, 1, a);

    (void)oox::run([](int& A) {
        A = 1;
        throw dummy_exception{};
    }, a);

    EXPECT_THROW((void)oox::wait_and_get(a), dummy_exception);
    EXPECT_THROW((void)oox::wait_and_get(b), dummy_exception);
}

TEST(OOX, ExceptionMultiOutputWriterAndReturn) {
    oox::var<int> a(oox::deferred);
    oox::var<int> r = oox::run([](int& A) -> int {
        A = 7;
        throw dummy_exception{};
    }, a);

    EXPECT_THROW((void)oox::wait_and_get(r), dummy_exception);
    EXPECT_THROW((void)oox::wait_and_get(a), dummy_exception);
}

TEST(OOX, ExceptionDiamondJoinSkipped) {
    oox::var<int> ok = oox::run([]() -> int { return 10; });
    oox::var<int> bad = oox::run([]() -> int { throw dummy_exception{}; });

    std::atomic<int> join_ran{0};
    oox::var<int> join = oox::run([&](int x, int y) -> int {
        join_ran.fetch_add(1);
        return x + y;
    }, ok, bad);

    EXPECT_THROW((void)oox::wait_and_get(join), dummy_exception);
    EXPECT_EQ(join_ran.load(), 0);
}

TEST(OOX, ExceptionForwardingThrowsBeforeReturningVar) {
    oox::var<int> a = 1;

    auto bad_forward = [](oox::var<int>) -> oox::var<int> {
        throw dummy_exception{};
    };

    oox::var<int> r = oox::run(bad_forward, a);

    EXPECT_THROW((void)oox::wait_and_get(r), dummy_exception);
}

TEST(OOX, ExceptionFailedVersionCanBeOverwritten) {
    oox::var<int> a(oox::deferred);

    (void)oox::run([](int& A) {
        A = 1;
        throw dummy_exception{};
    }, a);

    oox::var<int> b = oox::run(plus, 1, a);

    (void)oox::run([](int& A) { A = 2; }, a);

    ASSERT_EQ(oox::wait_and_get(a), 2);}


int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "-v") == 0)
            g_oox_verbose = true;
    }

    int err{0};
    try {
        err = RUN_ALL_TESTS();
    } catch (const std::exception& e) {
        REMARK("Error: %s", e.what());
    }
    return err;
}
