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

int plus(int a, int b) { return a+b; }

TEST(OOX, Simple) {
    const oox::var<int> a = oox::run(plus, 2, 3);
    oox::var<int> b = oox::run(plus, 1, a);
    ASSERT_EQ(oox::wait_and_get(a), 5);
    ASSERT_EQ(b.get(), 6);
}
TEST(OOX, DISABLED_Empty) {
    oox::var<int> a;
    oox::var<int> b = oox::run(plus, 1, a);
    oox::run([](int &A){ A = 2; }, a);
    ASSERT_EQ(oox::wait_and_get(a), 2);
    ASSERT_EQ(oox::wait_and_get(b), 3);
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

TEST(OOX, DISABLED_Consistency) {
    auto func = []() -> oox::var<int> {
        return oox::run(std::plus<int>(), 1, 1);
    };
    const auto res = oox::wait_and_get(oox::run(func));
    ASSERT_EQ(res, 2);
}

TEST(OOX, DISABLED_ConsistencyInfLoop) {
    const oox::var<int> tmp = 1;
    ASSERT_EQ(oox::wait_and_get(tmp), 1);
}

// TODO transfer to test_oox_deferred
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
    ASSERT_TRUE(bval == aval + 1 || bval == 2 || bval == 11);
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
