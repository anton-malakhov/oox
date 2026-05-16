// Copyright (C) 2026 OOX contributors
//
// SPDX-License-Identifier: Apache-2.0
//
// Context-level cancellation benchmarks for the ctx-from-main / PR28 branch.
// Distinct from cooperative branch cancellation (bench_branch_cancellation):
// here we cancel a per-run ctx::context that the scheduler observes, and the
// tasks observing the cancelled context propagate task_cancelled via
// wait_and_get.

#include <string>
#include <vector>

#include <benchmark/benchmark.h>

#include <oox/oox.h>

#include "bench_common.hpp"

#if !defined(OOX_CONTEXTS_ENABLED) || !OOX_CONTEXTS_ENABLED
#  error "bench_context_cancellation.cpp requires OOX_CONTEXTS_ENABLED"
#endif

namespace {

inline int leaf_compute(int seed, std::size_t work_iterations) {
    return static_cast<int>(bench_oox::spin_work(static_cast<std::uint64_t>(seed),
                                                 work_iterations));
}

bool wait_cancelled(oox::var<int>& v) {
    try {
        benchmark::DoNotOptimize(oox::wait_and_get(v));
        return false;
    } catch (const oox::task_cancelled&) {
        return true;
    }
}

void satisfy_gate(oox::ctx::context& c, oox::var<int>& gate) {
    oox::run([](int& g) { g = 1; }, gate);
    (void)c;
}

// --------------------------------------------------------------------------
// ContextCancel_Single
// --------------------------------------------------------------------------

void ContextCancel_Single(benchmark::State& state) {
    const std::size_t work = static_cast<std::size_t>(state.range(0));
    for (auto _ : state) {
        oox::ctx::context c;
        c.reset();

        oox::var<int> gate(oox::deferred);
        oox::var<int> result = oox::run(c, [work, &gate](int /*g*/) {
            return leaf_compute(1, work);
        }, gate);

        c.cancel();
        // satisfy gate so the chained task can be considered for scheduling
        satisfy_gate(c, gate);

        if (!wait_cancelled(result)) {
            state.SkipWithError("expected task_cancelled");
            break;
        }
    }
}
BENCHMARK(ContextCancel_Single)->Arg(0)->Arg(256)->Unit(benchmark::kMicrosecond);

// --------------------------------------------------------------------------
// ContextCancel_Chain<N>: root cancelled, chain propagates
// --------------------------------------------------------------------------

void ContextCancel_Chain(benchmark::State& state) {
    const int N = static_cast<int>(state.range(0));
    for (auto _ : state) {
        oox::ctx::context c;
        c.reset();

        oox::var<int> gate(oox::deferred);
        oox::var<int> head = oox::run(c, [&gate](int /*g*/) { return 1; }, gate);
        for (int i = 0; i < N; ++i) {
            head = oox::run(c, [](int prev) { return prev + 1; }, head);
        }

        c.cancel();
        satisfy_gate(c, gate);

        if (!wait_cancelled(head)) {
            state.SkipWithError("chain tail was not cancelled");
            break;
        }
    }
    state.SetItemsProcessed(state.iterations() * static_cast<int64_t>(N + 1));
}
BENCHMARK(ContextCancel_Chain)->Arg(1)->Arg(16)->Arg(64)->Unit(benchmark::kMicrosecond);

// --------------------------------------------------------------------------
// ContextCancel_Fanout<N>: cancelled producer feeds N consumers
// --------------------------------------------------------------------------

void ContextCancel_Fanout(benchmark::State& state) {
    const int N = static_cast<int>(state.range(0));
    const auto plus = [](int x, int y) { return x + y; };

    for (auto _ : state) {
        oox::ctx::context c;
        c.reset();

        oox::var<int> gate(oox::deferred);
        oox::var<int> producer = oox::run(c, [&gate](int /*g*/) { return 1; }, gate);

        std::vector<oox::var<int>> leaves;
        leaves.reserve(static_cast<std::size_t>(N));
        for (int i = 0; i < N; ++i) {
            leaves.push_back(oox::run(c, plus, i, producer));
        }

        c.cancel();
        satisfy_gate(c, gate);

        bool all_cancelled = true;
        for (auto& leaf : leaves) {
            all_cancelled = wait_cancelled(leaf) && all_cancelled;
        }
        if (!all_cancelled) {
            state.SkipWithError("not all leaves cancelled");
            break;
        }
    }
    state.SetItemsProcessed(state.iterations() * static_cast<int64_t>(N + 1));
}
BENCHMARK(ContextCancel_Fanout)->Arg(2)->Arg(16)->Arg(64)->Unit(benchmark::kMicrosecond);

} // namespace

BENCHMARK_MAIN();
