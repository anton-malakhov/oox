// Copyright (C) 2026 OOX contributors
//
// SPDX-License-Identifier: Apache-2.0
//
// Success-path benchmarks for the OOX benchmark suite. These benchmarks
// measure normal, exception-free graph execution. They are intentionally
// branch-agnostic: the same source compiles on main, PR24, PR28 and PR29.
// Branches with per-target exception policies build this file in
// support_off / support_on_default_noexc / support_on_default_exc modes via
// CMake compile definitions so that pay-as-you-go overhead can be measured.

#include <string>
#include <vector>

#include <benchmark/benchmark.h>

#define BENCH_STR_(x) #x
#define BENCH_STR(x) BENCH_STR_(x)

#if defined(PARALLEL)
inline const std::string bench_parallel_str = BENCH_STR(PARALLEL);
#else
inline const std::string bench_parallel_str = "exe";
#endif

#if defined(OOX_EXCEPTION_POLICY_STR)
inline const std::string bench_policy_str = BENCH_STR(OOX_EXCEPTION_POLICY_STR);
#else
inline const std::string bench_policy_str = "default";
#endif

#include <oox/oox.h>

#include "bench_common.hpp"

namespace {

// A non-trivial functor body that the compiler cannot constant-fold away.
inline int leaf_compute(int seed, std::size_t work_iterations) {
    return static_cast<int>(bench_oox::spin_work(static_cast<std::uint64_t>(seed),
                                                 work_iterations));
}

// --------------------------------------------------------------------------
// Single
// --------------------------------------------------------------------------

void Success_Single(benchmark::State& state) {
    const std::size_t work = static_cast<std::size_t>(state.range(0));
    for (auto _ : state) {
        auto root = oox::run([work] { return leaf_compute(1, work); });
        auto value = oox::wait_and_get(root);
        benchmark::DoNotOptimize(value);
    }
}

namespace { constexpr int kFastIters = 500; }

BENCHMARK(Success_Single)
    ->Arg(0)->Arg(256)
    ->Iterations(kFastIters)
    ->Unit(benchmark::kMicrosecond);

// --------------------------------------------------------------------------
// Chain<N>: root -> t1 -> t2 -> ... -> tN
// --------------------------------------------------------------------------

void Success_Chain(benchmark::State& state) {
    const int N = static_cast<int>(state.range(0));
    const std::size_t work = static_cast<std::size_t>(state.range(1));
    for (auto _ : state) {
        oox::var<int> head = oox::run([work] { return leaf_compute(1, work); });
        for (int i = 0; i < N; ++i) {
            head = oox::run([work](int prev) { return prev + leaf_compute(prev, work); },
                            std::move(head));
        }
        auto value = oox::wait_and_get(head);
        benchmark::DoNotOptimize(value);
    }
}
BENCHMARK(Success_Chain)
    ->ArgsProduct({{1, 8, 64}, {0, 256}})
    ->Iterations(kFastIters)
    ->Unit(benchmark::kMicrosecond);

// --------------------------------------------------------------------------
// Fanout<N>: one producer feeds N consumers
// --------------------------------------------------------------------------

void Success_Fanout(benchmark::State& state) {
    const int N = static_cast<int>(state.range(0));
    const std::size_t work = static_cast<std::size_t>(state.range(1));
    for (auto _ : state) {
        oox::var<int> producer = oox::run([work] { return leaf_compute(1, work); });

        std::vector<oox::var<int>> leaves;
        leaves.reserve(static_cast<std::size_t>(N));
        for (int i = 0; i < N; ++i) {
            leaves.push_back(oox::run(
                [work, i](int v) { return v + leaf_compute(i, work); },
                producer));
        }

        int acc = 0;
        for (auto& leaf : leaves) {
            acc += oox::wait_and_get(leaf);
        }
        benchmark::DoNotOptimize(acc);
    }
}
BENCHMARK(Success_Fanout)
    ->ArgsProduct({{1, 8, 64}, {0, 256}})
    ->Iterations(kFastIters)
    ->Unit(benchmark::kMicrosecond);

// --------------------------------------------------------------------------
// Fanin<N>: N producers feed a single binary-tree-style reducer
// --------------------------------------------------------------------------

void Success_Fanin(benchmark::State& state) {
    const int N = static_cast<int>(state.range(0));
    const std::size_t work = static_cast<std::size_t>(state.range(1));
    const auto plus = [](int a, int b) { return a + b; };

    for (auto _ : state) {
        std::vector<oox::var<int>> level;
        level.reserve(static_cast<std::size_t>(N));
        for (int i = 0; i < N; ++i) {
            level.push_back(oox::run([work, i] { return leaf_compute(i, work); }));
        }
        while (level.size() > 1) {
            std::vector<oox::var<int>> next;
            next.reserve((level.size() + 1) / 2);
            for (std::size_t i = 0; i + 1 < level.size(); i += 2) {
                next.push_back(oox::run(plus,
                                        std::move(level[i]),
                                        std::move(level[i + 1])));
            }
            if (level.size() % 2 == 1) {
                next.push_back(std::move(level.back()));
            }
            level = std::move(next);
        }
        auto value = oox::wait_and_get(level.front());
        benchmark::DoNotOptimize(value);
    }
}
BENCHMARK(Success_Fanin)
    ->ArgsProduct({{2, 16, 64}, {0, 256}})
    ->Iterations(kFastIters)
    ->Unit(benchmark::kMicrosecond);

// --------------------------------------------------------------------------
// Diamond<Levels>: split-merge graph of given depth
// At each level the width doubles via two-way split, then we merge back down.
// --------------------------------------------------------------------------

void Success_Diamond(benchmark::State& state) {
    const int Levels = static_cast<int>(state.range(0));
    const std::size_t work = static_cast<std::size_t>(state.range(1));
    const auto plus = [](int a, int b) { return a + b; };

    for (auto _ : state) {
        std::vector<oox::var<int>> level;
        level.push_back(oox::run([work] { return leaf_compute(1, work); }));

        // Split: each existing node spawns two children that depend on it.
        for (int l = 0; l < Levels; ++l) {
            std::vector<oox::var<int>> next;
            next.reserve(level.size() * 2);
            for (auto& parent : level) {
                next.push_back(oox::run(
                    [work](int v) { return v + leaf_compute(v, work); }, parent));
                next.push_back(oox::run(
                    [work](int v) { return v + leaf_compute(v + 1, work); }, parent));
            }
            level = std::move(next);
        }

        // Merge back down.
        while (level.size() > 1) {
            std::vector<oox::var<int>> next;
            next.reserve(level.size() / 2);
            for (std::size_t i = 0; i + 1 < level.size(); i += 2) {
                next.push_back(oox::run(plus,
                                        std::move(level[i]),
                                        std::move(level[i + 1])));
            }
            level = std::move(next);
        }

        auto value = oox::wait_and_get(level.front());
        benchmark::DoNotOptimize(value);
    }
}
BENCHMARK(Success_Diamond)
    ->ArgsProduct({{2, 4}, {0}})
    ->Iterations(kFastIters)
    ->Unit(benchmark::kMicrosecond);

} // namespace

BENCHMARK_MAIN();
