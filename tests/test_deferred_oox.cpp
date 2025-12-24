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
