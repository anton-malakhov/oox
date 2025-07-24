// Copyright (C) 2021 Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

#include <gtest/gtest.h>

#include <iostream>
#include <numeric>
#include <vector>
//#include <tbb/concurrent_vector.h>

bool g_oox_verbose = false;
#define println(s, ...) fprintf(stderr, s "\n",  __VA_ARGS__)
#define __OOX_TRACE if (g_oox_verbose) println
#define __OOX_ASSERT(b, m) if(!(b)) { println("OOX assertion failed: " #b " at line %d: %s", __LINE__, m); abort(); }
#define __OOX_ASSERT_EX(b, m) __OOX_ASSERT(b,m)

#include <oox/oox.h>

#define REMARK println
#define ASSERT(b, m) ASSERT_TRUE(b) << (m)

/////////////////////////////////////// EXAMPLES ////////////////////////////////////////

#include "../examples/fibonacci.h"
#include "../examples/filesystem.h"
#include "../examples/wavefront.h"

namespace ArchSample {
    // example from original OOX
    typedef int T;
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
TEST(OOX, DISABLED_Empty) { // TODO!
    oox::var<int> a; // TODO: = oox::deferred;
    oox::var<int> b = oox::run(plus, 1, a);
    oox::run([](int &A){ A = 2; }, a); // TODO: define another assignment operator rule?
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
    int x = 25;
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

int broken_func() {
    throw std::runtime_error("exception in broken_func");
    return 0;
}

TEST(OOX, Exception) {
    #if OOX_SERIAL_DEBUG
    ASSERT_THROW(auto exec = oox::run(broken_func), std::runtime_error);
    #else
    auto exec = oox::run(broken_func);
    ASSERT_THROW(oox::wait_and_get(exec), std::runtime_error);
    #endif
}

int empty_func() {
    return 1;
}

TEST(OOX, Cancellation) {
    auto exec = oox::run(empty_func);
    oox::cancel(exec);
    ASSERT_NO_THROW(oox::wait_and_get(exec));
}

std::vector<int> g_fib_levels;
const int cancellation_index = 11;

oox::var<int> Fib_with_exception(int n) {
    g_fib_levels[n]++;
    if (n <= cancellation_index) { throw std::runtime_error("detected n is 7"); }
    if(n < 2) return n;
    return oox::run(std::plus<int>(), oox::run(Fib_with_exception, n-1), oox::run(Fib_with_exception, n-2) );
}

TEST(OOX, Fib_Exception) {
    int n = 28;
    g_fib_levels.clear();
    g_fib_levels.resize(n + 1, 0);
    ASSERT_THROW(oox::wait_and_get(Fib_with_exception(n)), std::runtime_error);
    for(int index = 0; index < cancellation_index - 1; index++){
        ASSERT_EQ(g_fib_levels[index], 0);
    }
    if (cancellation_index == 0) {
        ASSERT_TRUE(g_fib_levels[cancellation_index]);
    } else {
        ASSERT_TRUE(g_fib_levels[cancellation_index] + g_fib_levels[cancellation_index - 1]);
    }
}

oox::var<int> Fib_with_cancellation(int n) {
    g_fib_levels[n]++;
    if (n <= cancellation_index) { oox::cancel(); }
    if(n < 2) return n;
    return oox::run(std::plus<int>(), oox::run(Fib_with_cancellation, n-1), oox::run(Fib_with_cancellation, n-2) );
}

TEST(OOX, Fib_Cancellation) {
    int n = 28;
    g_fib_levels.clear();
    g_fib_levels.resize(n + 1, 0);
    ASSERT_NO_THROW(oox::wait_and_get(Fib_with_cancellation(n)));
    for(int index = 0; index < cancellation_index - 1; index++){
        ASSERT_EQ(g_fib_levels[index], 0);
    }
    if (cancellation_index == 0) {
        ASSERT_TRUE(g_fib_levels[cancellation_index]);
    } else {
        ASSERT_TRUE(g_fib_levels[cancellation_index] + g_fib_levels[cancellation_index - 1]);
    }
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
