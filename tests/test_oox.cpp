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
#if OOX_ENABLE_EXCEPTIONS
#include <stdexcept>
#include <atomic>
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



/////////////////////////////////////// DEFERRED ////////////////////////////////////////

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

/////////////////////////////////////// EXCEPTIONS ////////////////////////////////////////

#if OOX_ENABLE_EXCEPTIONS
struct dummy_exception final : std::exception {
    [[nodiscard]] const char* what() const noexcept override { return "dummy throw"; }
};

TEST(OOX, ExceptionReturnRethrowsOriginal) {
    oox::var<int> a = oox::run([]() -> int {
        throw dummy_exception{};
    });
    EXPECT_THROW(oox::wait_and_get(a), dummy_exception);
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

    EXPECT_THROW(oox::wait_and_get(c), dummy_exception);
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
    EXPECT_THROW(oox::wait_and_get(b), dummy_exception);
}

TEST(OOX, ExceptionDeferredWriterPoison) {
    oox::var<int> a(oox::deferred);
    oox::var<int> b = oox::run(plus, 1, a);

    oox::run([](int& A) {
        A = 1;
        throw dummy_exception{};
    }, a);

    EXPECT_THROW(oox::wait_and_get(a), oox::cancelled_by_exception);
    EXPECT_THROW(oox::wait_and_get(b), oox::cancelled_by_exception);
}

TEST(OOX, ExceptionLateConsumerAfterOutputVarCancelled) {
    oox::var<int> a(oox::deferred);

    oox::run([](int& A) {
        A = 1;
        throw dummy_exception{};
    }, a);

    EXPECT_THROW(oox::wait_for_all(a), oox::cancelled_by_exception);

    oox::var<int> b = oox::run(plus, 1, a);
    EXPECT_THROW(oox::wait_and_get(b), oox::cancelled_by_exception);
}

TEST(OOX, ExceptionExplicitVarCancelSkipsBodyAndPropagates) {
    oox::var<int> gate(oox::deferred);
    std::atomic<int> ran_a{0};
    std::atomic<int> ran_b{0};

    oox::var<int> a = oox::run([&](int x) -> int {
        ran_a.fetch_add(1);
        return x + 1;
    }, gate);
    oox::var<int> b = oox::run([&](int x) -> int {
        ran_b.fetch_add(1);
        return x + 1;
    }, a);

    a.cancel();
    oox::run([](int& g) { g = 1; }, gate);

    EXPECT_THROW(oox::wait_and_get(a), oox::cancelled_by_user);
    EXPECT_THROW(oox::wait_and_get(b), oox::cancelled_by_user);
    EXPECT_EQ(ran_a.load(), 0);
    EXPECT_EQ(ran_b.load(), 0);
}

TEST(OOX, ExceptionExplicitTaskCancelSkipsBodyAndPropagates) {
    oox::var<int> gate(oox::deferred);
    std::atomic<int> ran_a{0};
    std::atomic<int> ran_b{0};

    oox::var<int> a = oox::run([&](int x) -> int {
        ran_a.fetch_add(1);
        return x + 1;
    }, gate);
    oox::var<int> b = oox::run([&](int x) -> int {
        ran_b.fetch_add(1);
        return x + 1;
    }, a);

    ASSERT_TRUE(a.current_task != nullptr);
    a.current_task->cancel();
    oox::run([](int& g) { g = 1; }, gate);

    EXPECT_THROW(oox::wait_and_get(a), oox::cancelled_by_user);
    EXPECT_THROW(oox::wait_and_get(b), oox::cancelled_by_user);
    EXPECT_EQ(ran_a.load(), 0);
    EXPECT_EQ(ran_b.load(), 0);
}

TEST(OOX, ExceptionExplicitNodeCancelSkipsBody) {
    oox::var<int> gate(oox::deferred);
    std::atomic<int> ran{0};

    oox::node n = oox::run([&](int x) {
        ran.fetch_add(x);
    }, gate);

    n.cancel();
    oox::run([](int& g) { g = 1; }, gate);

    EXPECT_THROW(oox::wait_for_all(n), oox::cancelled_by_user);
    EXPECT_EQ(ran.load(), 0);
}

TEST(OOX, ExceptionExplicitCancelRealExceptionWins) {
    oox::var<int> gate(oox::deferred);
    oox::var<int> bad = oox::run([](int) -> int {
        throw dummy_exception{};
    }, gate);
    oox::var<int> out = oox::run(plus, 1, bad);

    out.cancel();
    oox::run([](int& g) { g = 1; }, gate);

    EXPECT_THROW(oox::wait_and_get(out), dummy_exception);
}

TEST(OOX, ExceptionExplicitCancelCanBeRecoveredByWriter) {
    oox::var<int> a(oox::deferred);
    oox::var<int> stale = oox::run(plus, 1, a);

    a.cancel();
    oox::run([](int& A) { A = 10; }, a);
    oox::var<int> fresh = oox::run(plus, 2, a);

    EXPECT_EQ(oox::wait_and_get(a), 10);
    EXPECT_EQ(oox::wait_and_get(stale), 11);
    EXPECT_EQ(oox::wait_and_get(fresh), 12);
}

TEST(OOX, ExceptionExplicitCancelAfterCompletionIsNoop) {
    oox::var<int> a = oox::run([]() -> int { return 5; });

    EXPECT_EQ(oox::wait_and_get(a), 5);
    a.cancel();
    EXPECT_EQ(oox::wait_and_get(a), 5);
}

TEST(OOX, ExceptionCancelLargeTreeIntermediateSubtree) {
    constexpr int kDepth = 8;
    constexpr int kCancelDepth = 3;
    constexpr int kCancelIndex = 5;
    static_assert(kCancelDepth < kDepth, "cancelled node must be internal");

    auto in_cancel_subtree = [](int depth, int index) -> bool {
        if (depth < kCancelDepth) {
            return false;
        }
        return (index >> (depth - kCancelDepth)) == kCancelIndex;
    };

    oox::var<int> gate(oox::deferred);
    std::atomic<int> cancelled_branch_runs{0};
    std::atomic<int> live_branch_runs{0};

    std::vector<std::vector<oox::var<int>>> levels(kDepth + 1);
    levels[0].reserve(1);
    levels[0].push_back(oox::run([&](int x) -> int {
        if (in_cancel_subtree(0, 0)) {
            cancelled_branch_runs.fetch_add(1);
        } else {
            live_branch_runs.fetch_add(1);
        }
        return x + 1;
    }, gate));

    int expected_cancelled_runs = 0;
    int expected_live_runs = 1;

    for (int depth = 1; depth <= kDepth; ++depth) {
        levels[depth].reserve(1 << depth);
        const int parent_count = 1 << (depth - 1);
        for (int i = 0; i < parent_count; ++i) {
            auto& parent = levels[depth - 1][i];

            const int left_index = 2 * i;
            const int right_index = left_index + 1;

            if (in_cancel_subtree(depth, left_index)) {
                ++expected_cancelled_runs;
            } else {
                ++expected_live_runs;
            }
            if (in_cancel_subtree(depth, right_index)) {
                ++expected_cancelled_runs;
            } else {
                ++expected_live_runs;
            }

            levels[depth].push_back(oox::run([&, depth, left_index](int x) -> int {
                if (in_cancel_subtree(depth, left_index)) {
                    cancelled_branch_runs.fetch_add(1);
                } else {
                    live_branch_runs.fetch_add(1);
                }
                return x + 1;
            }, parent));

            levels[depth].push_back(oox::run([&, depth, right_index](int x) -> int {
                if (in_cancel_subtree(depth, right_index)) {
                    cancelled_branch_runs.fetch_add(1);
                } else {
                    live_branch_runs.fetch_add(1);
                }
                return x + 2;
            }, parent));
        }
    }

    levels[kCancelDepth][kCancelIndex].cancel();
    oox::run([](int& g) { g = 1; }, gate);

    int cancelled_leaf_count = 0;
    int live_leaf_count = 0;
    for (int i = 0; i < (1 << kDepth); ++i) {
        if (in_cancel_subtree(kDepth, i)) {
            ++cancelled_leaf_count;
            EXPECT_THROW(oox::wait_and_get(levels[kDepth][i]), oox::cancelled_by_user);
        } else {
            ++live_leaf_count;
            int value = 0;
            EXPECT_NO_THROW(value = oox::wait_and_get(levels[kDepth][i]));
            EXPECT_GT(value, 0);
        }
    }

    EXPECT_GT(cancelled_leaf_count, 0);
    EXPECT_GT(live_leaf_count, 0);
    EXPECT_EQ(cancelled_branch_runs.load(), 0);
    EXPECT_EQ(live_branch_runs.load(), expected_live_runs);
    EXPECT_GT(expected_cancelled_runs, 0);
}

TEST(OOX, ExceptionCancelIntermediateNodeAfterUpstreamReady) {
    oox::var<int> gate1(oox::deferred);
    oox::var<int> gate2(oox::deferred);

    std::atomic<int> target_ran{0};
    std::atomic<int> child_ran{0};
    std::atomic<int> sibling_ran{0};

    oox::var<int> root = oox::run([](int x) -> int { return x + 1; }, gate1);
    oox::var<int> target = oox::run([&](int x, int y) -> int {
        target_ran.fetch_add(1);
        return x + y;
    }, root, gate2);
    oox::var<int> child = oox::run([&](int x) -> int {
        child_ran.fetch_add(1);
        return x + 10;
    }, target);
    oox::var<int> sibling = oox::run([&](int x) -> int {
        sibling_ran.fetch_add(1);
        return x + 2;
    }, root);

    oox::run([](int& g) { g = 3; }, gate1);
    EXPECT_EQ(oox::wait_and_get(root), 4);

    ASSERT_TRUE(target.current_task != nullptr);
    target.current_task->cancel();

    oox::run([](int& g) { g = 5; }, gate2);

    EXPECT_THROW(oox::wait_and_get(target), oox::cancelled_by_user);
    EXPECT_THROW(oox::wait_and_get(child), oox::cancelled_by_user);
    EXPECT_EQ(oox::wait_and_get(sibling), 6);
    EXPECT_EQ(target_ran.load(), 0);
    EXPECT_EQ(child_ran.load(), 0);
    EXPECT_EQ(sibling_ran.load(), 1);
}

TEST(OOX, ExceptionThrowLargeTreeIntermediateSubtree) {
    constexpr int kDepth = 8;
    constexpr int kThrowDepth = 3;
    constexpr int kThrowIndex = 5;
    static_assert(kThrowDepth < kDepth, "throwing node must be internal");

    auto is_throw_node = [](int depth, int index) -> bool {
        return depth == kThrowDepth && index == kThrowIndex;
    };
    auto in_throw_subtree = [](int depth, int index) -> bool {
        if (depth < kThrowDepth) {
            return false;
        }
        return (index >> (depth - kThrowDepth)) == kThrowIndex;
    };

    oox::var<int> gate(oox::deferred);
    std::atomic<int> throwing_node_runs{0};
    std::atomic<int> blocked_subtree_runs{0};
    std::atomic<int> live_branch_runs{0};

    std::vector<std::vector<oox::var<int>>> levels(kDepth + 1);
    levels[0].reserve(1);
    levels[0].push_back(oox::run([&](int x) -> int {
        live_branch_runs.fetch_add(1);
        return x + 1;
    }, gate));

    int expected_live_runs = 1;
    int expected_blocked_descendants = 0;

    for (int depth = 1; depth <= kDepth; ++depth) {
        levels[depth].reserve(1 << depth);
        const int parent_count = 1 << (depth - 1);
        for (int i = 0; i < parent_count; ++i) {
            auto& parent = levels[depth - 1][i];

            const int left_index = 2 * i;
            const int right_index = left_index + 1;

            if (is_throw_node(depth, left_index)) {
                // The throwing node itself should execute once and throw.
            } else if (in_throw_subtree(depth, left_index)) {
                ++expected_blocked_descendants;
            } else {
                ++expected_live_runs;
            }
            if (is_throw_node(depth, right_index)) {
                // The throwing node itself should execute once and throw.
            } else if (in_throw_subtree(depth, right_index)) {
                ++expected_blocked_descendants;
            } else {
                ++expected_live_runs;
            }

            levels[depth].push_back(oox::run([&, depth, left_index](int x) -> int {
                if (is_throw_node(depth, left_index)) {
                    throwing_node_runs.fetch_add(1);
                    throw dummy_exception{};
                }
                if (in_throw_subtree(depth, left_index)) {
                    blocked_subtree_runs.fetch_add(1);
                } else {
                    live_branch_runs.fetch_add(1);
                }
                return x + 1;
            }, parent));

            levels[depth].push_back(oox::run([&, depth, right_index](int x) -> int {
                if (is_throw_node(depth, right_index)) {
                    throwing_node_runs.fetch_add(1);
                    throw dummy_exception{};
                }
                if (in_throw_subtree(depth, right_index)) {
                    blocked_subtree_runs.fetch_add(1);
                } else {
                    live_branch_runs.fetch_add(1);
                }
                return x + 2;
            }, parent));
        }
    }

    oox::run([](int& g) { g = 1; }, gate);

    int throwing_leaf_count = 0;
    int live_leaf_count = 0;
    for (int i = 0; i < (1 << kDepth); ++i) {
        if (in_throw_subtree(kDepth, i)) {
            ++throwing_leaf_count;
            EXPECT_THROW(oox::wait_and_get(levels[kDepth][i]), dummy_exception);
        } else {
            ++live_leaf_count;
            int value = 0;
            EXPECT_NO_THROW(value = oox::wait_and_get(levels[kDepth][i]));
            EXPECT_GT(value, 0);
        }
    }

    EXPECT_GT(throwing_leaf_count, 0);
    EXPECT_GT(live_leaf_count, 0);
    EXPECT_EQ(throwing_node_runs.load(), 1);
    EXPECT_EQ(blocked_subtree_runs.load(), 0);
    EXPECT_EQ(live_branch_runs.load(), expected_live_runs);
    EXPECT_GT(expected_blocked_descendants, 0);
}

TEST(OOX, ExceptionThrowLargeLayeredDagPartitionPropagation) {
    constexpr int kWidth = 32;
    constexpr int kLayers = 6;
    static_assert((kWidth % 2) == 0, "width must be even");
    constexpr int kHalf = kWidth / 2;

    oox::var<int> gate(oox::deferred);
    std::atomic<int> source_bad_runs{0};
    std::atomic<int> source_good_runs{0};
    std::atomic<int> bad_partition_runs{0};
    std::atomic<int> good_partition_runs{0};
    std::atomic<int> mixed_sink_runs{0};
    std::atomic<int> good_sink_runs{0};

    oox::var<int> bad_source = oox::run([&](int) -> int {
        source_bad_runs.fetch_add(1);
        throw dummy_exception{};
    }, gate);
    oox::var<int> good_source = oox::run([&](int x) -> int {
        source_good_runs.fetch_add(1);
        return x + 1;
    }, gate);

    std::vector<oox::var<int>> current;
    current.reserve(kWidth);
    for (int i = 0; i < kHalf; ++i) {
        current.push_back(oox::run([&](int x) -> int {
            bad_partition_runs.fetch_add(1);
            return x + 1;
        }, bad_source));
    }
    for (int i = 0; i < kHalf; ++i) {
        current.push_back(oox::run([&](int x) -> int {
            good_partition_runs.fetch_add(1);
            return x + 1;
        }, good_source));
    }

    int expected_good_runs = kHalf;

    for (int layer = 1; layer <= kLayers; ++layer) {
        std::vector<oox::var<int>> next;
        next.reserve(kWidth);

        for (int i = 0; i < kHalf; ++i) {
            const int j = (i + 1) % kHalf;
            next.push_back(oox::run([&](int a, int b) -> int {
                bad_partition_runs.fetch_add(1);
                return a + b + 1;
            }, current[i], current[j]));
        }

        for (int i = 0; i < kHalf; ++i) {
            const int idx = kHalf + i;
            const int jdx = kHalf + ((i + 1) % kHalf);
            next.push_back(oox::run([&](int a, int b) -> int {
                good_partition_runs.fetch_add(1);
                return a + b + 1;
            }, current[idx], current[jdx]));
        }

        expected_good_runs += kHalf;
        current = std::move(next);
    }

    oox::var<int> mixed_sink = oox::run([&](int a, int b) -> int {
        mixed_sink_runs.fetch_add(1);
        return a + b;
    }, current[0], current[kHalf]);

    oox::var<int> good_sink = oox::run([&](int a, int b) -> int {
        good_sink_runs.fetch_add(1);
        return a + b;
    }, current[kHalf], current[kHalf + 1]);

    oox::run([](int& g) { g = 1; }, gate);

    EXPECT_THROW(oox::wait_and_get(mixed_sink), dummy_exception);

    int good_sink_value = 0;
    EXPECT_NO_THROW(good_sink_value = oox::wait_and_get(good_sink));
    EXPECT_GT(good_sink_value, 0);

    for (int i = 0; i < kHalf; ++i) {
        EXPECT_THROW(oox::wait_and_get(current[i]), dummy_exception);
    }
    for (int i = kHalf; i < kWidth; ++i) {
        int value = 0;
        EXPECT_NO_THROW(value = oox::wait_and_get(current[i]));
        EXPECT_GT(value, 0);
    }

    EXPECT_EQ(source_bad_runs.load(), 1);
    EXPECT_EQ(source_good_runs.load(), 1);
    EXPECT_EQ(bad_partition_runs.load(), 0);
    EXPECT_EQ(good_partition_runs.load(), expected_good_runs);
    EXPECT_EQ(mixed_sink_runs.load(), 0);
    EXPECT_EQ(good_sink_runs.load(), 1);
}

TEST(OOX, ExceptionMultiOutputWriterAndReturn) {
    oox::var<int> a(oox::deferred);
    oox::var<int> r = oox::run([](int& A) -> int {
        A = 7;
        throw dummy_exception{};
    }, a);

    EXPECT_THROW(oox::wait_and_get(r), dummy_exception);
    EXPECT_THROW(oox::wait_and_get(a), oox::cancelled_by_exception);
}

TEST(OOX, ExceptionConstRefInputNotCancelled) {
    oox::var<int> a(oox::deferred);

    oox::run([](int& A) { A = 7; }, a);

    oox::var<int> r = oox::run([]([[maybe_unused]] const int& A) -> int {
        throw dummy_exception{};
    }, a);

    EXPECT_THROW(oox::wait_and_get(r), dummy_exception);
    EXPECT_EQ(oox::wait_and_get(a), 7);
}

TEST(OOX, ExceptionConstRefReaderWithWriterCancelsOnlyOutput) {
    oox::var<int> input = oox::run([]() -> int { return 3; });
    oox::var<int> output(oox::deferred);

    oox::var<int> r = oox::run([](int& Out, const int& In) -> int {
        Out = In + 1;
        throw dummy_exception{};
    }, output, input);

    EXPECT_THROW(oox::wait_and_get(r), dummy_exception);
    EXPECT_THROW(oox::wait_and_get(output), oox::cancelled_by_exception);
    EXPECT_EQ(oox::wait_and_get(input), 3);
}

TEST(OOX, ExceptionRealOverridesCancelled) {
    oox::var<int> cancelled_input(oox::deferred);
    oox::var<int> gate(oox::deferred);

    oox::var<int> throwing_input = oox::run([](int) -> int {
        throw dummy_exception{};
    }, gate);

    oox::var<int> join = oox::run([](int x, int y) -> int {
        return x + y;
    }, cancelled_input, throwing_input);

    oox::run([](int& out) {
        out = 1;
        throw dummy_exception{};
    }, cancelled_input);

    oox::run([](int& out) { out = 1; }, gate);

    EXPECT_THROW(oox::wait_and_get(join), dummy_exception);
}

TEST(OOX, ExceptionDiamondJoinSkipped) {
    oox::var<int> ok = oox::run([]() -> int { return 10; });
    oox::var<int> bad = oox::run([]() -> int { throw dummy_exception{}; });

    std::atomic<int> join_ran{0};
    oox::var<int> join = oox::run([&](int x, int y) -> int {
        join_ran.fetch_add(1);
        return x + y;
    }, ok, bad);

    EXPECT_THROW(oox::wait_and_get(join), dummy_exception);
    EXPECT_EQ(join_ran.load(), 0);
}

TEST(OOX, ExceptionForwardingThrowsBeforeReturningVar) {
    oox::var<int> a = 1;

    auto bad_forward = [](oox::var<int>) -> oox::var<int> {
        throw dummy_exception{};
    };

    oox::var<int> r = oox::run(bad_forward, a);

    EXPECT_THROW(oox::wait_and_get(r), dummy_exception);
}

TEST(OOX, ExceptionFailedVersionCanBeOverwritten) {
    oox::var<int> a(oox::deferred);

    oox::run([](int& A) {
        A = 1;
        throw dummy_exception{};
    }, a);

    oox::var<int> b = oox::run(plus, 1, a);

    oox::run([](int& A) { A = 2; }, a);

    ASSERT_EQ(oox::wait_and_get(a), 2);
}
#endif


int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "-v") == 0)
            g_oox_verbose = true;
    }

    int err{0};
#if OOX_ENABLE_EXCEPTIONS
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
