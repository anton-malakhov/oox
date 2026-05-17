// Copyright (C) 2026 OOX contributors
//
// SPDX-License-Identifier: Apache-2.0
//
// Cooperative branch cancellation benchmarks. Models a parallel search where
// many branches start, one branch finds the answer, and the others should
// stop as soon as possible. This is distinct from graph-level cancellation
// of OOX values; the cancellation mechanism here is a benchmark-local
// std::atomic token shared by all branches, so the benchmark runs on every
// branch including main.

#include <atomic>
#include <cstdint>
#include <string>
#include <vector>

#include <benchmark/benchmark.h>

#include <oox/oox.h>

#include "bench_common.hpp"

namespace {

constexpr std::size_t kBaseWorkPerStep = 1; // size of the inner spin

struct branch_config {
    int branches;
    std::size_t work_per_branch;
    int winner_index;     // -1 means "no winner / no cancel"
    int poll_interval;    // how often a branch checks the token
};

void run_branch(int branch_index,
                const branch_config& cfg,
                bench_oox::cancellation_token token,
                bench_oox::cancellation_source* source,
                std::atomic<int>& winner,
                std::atomic<std::uint64_t>& visited) {
    const std::size_t winner_iteration = cfg.work_per_branch / 2;
    for (std::size_t i = 0; i < cfg.work_per_branch; ++i) {
        if (cfg.poll_interval > 0 &&
            (i % static_cast<std::size_t>(cfg.poll_interval)) == 0 &&
            token.stop_requested()) {
            return;
        }
        benchmark::DoNotOptimize(bench_oox::spin_work(
            static_cast<std::uint64_t>(branch_index) + i, kBaseWorkPerStep));
        visited.fetch_add(1, std::memory_order_relaxed);

        if (source != nullptr && branch_index == cfg.winner_index &&
            i == winner_iteration) {
            int expected = -1;
            if (winner.compare_exchange_strong(expected, branch_index)) {
                source->request_cancel();
            }
            return;
        }
    }
}

void run_token_benchmark(benchmark::State& state, int winner_index) {
    branch_config cfg{
        static_cast<int>(state.range(0)),
        static_cast<std::size_t>(state.range(1)),
        winner_index,
        static_cast<int>(state.range(2)),
    };
    if (cfg.winner_index < 0 && state.range(3) != 0) {
        cfg.winner_index = cfg.branches / 2;
    }

    std::uint64_t total_visited = 0;
    int last_winner = -1;
    for (auto _ : state) {
        std::atomic<int> winner{-1};
        std::atomic<std::uint64_t> visited{0};
        bench_oox::cancellation_source source;
        auto token = source.token();
        // The winner_index < 0 path is the no-cancel baseline.
        bench_oox::cancellation_source* maybe_source =
            cfg.winner_index >= 0 ? &source : nullptr;

        std::vector<oox::var<int>> tasks;
        tasks.reserve(static_cast<std::size_t>(cfg.branches));
        for (int b = 0; b < cfg.branches; ++b) {
            tasks.push_back(oox::run(
                [b, cfg, token, maybe_source, &winner, &visited]() mutable {
                    run_branch(b, cfg, token, maybe_source, winner, visited);
                    return b;
                }));
        }
        for (auto& t : tasks) {
            benchmark::DoNotOptimize(oox::wait_and_get(t));
        }

        last_winner = winner.load(std::memory_order_relaxed);
        total_visited += visited.load(std::memory_order_relaxed);
    }

    state.counters["branches"]      = cfg.branches;
    state.counters["work"]          = static_cast<double>(cfg.work_per_branch);
    state.counters["poll"]          = cfg.poll_interval;
    state.counters["winner"]        = last_winner;
    state.counters["visited_total"] = static_cast<double>(total_visited);
    state.counters["visited_per_iter"] =
        static_cast<double>(total_visited) / static_cast<double>(state.iterations());
}

void Branch_NoCancel_AllComplete(benchmark::State& state) {
    run_token_benchmark(state, /*winner=*/-1);
}
BENCHMARK(Branch_NoCancel_AllComplete)
    ->ArgsProduct({{4, 16}, {256}, {16}, {0}})
    ->Iterations(2500)
    ->Unit(benchmark::kMicrosecond);

void Branch_TokenCancel_FirstWinner(benchmark::State& state) {
    run_token_benchmark(state, /*winner=*/0);
}
BENCHMARK(Branch_TokenCancel_FirstWinner)
    ->ArgsProduct({{4, 16}, {256}, {16}, {0}})
    ->Iterations(2500)
    ->Unit(benchmark::kMicrosecond);

void Branch_TokenCancel_MiddleWinner(benchmark::State& state) {
    const int branches = static_cast<int>(state.range(0));
    run_token_benchmark(state, /*winner=*/branches / 2);
}
BENCHMARK(Branch_TokenCancel_MiddleWinner)
    ->ArgsProduct({{4, 16}, {256}, {16}, {0}})
    ->Iterations(2500)
    ->Unit(benchmark::kMicrosecond);

void Branch_TokenCancel_LastWinner(benchmark::State& state) {
    const int branches = static_cast<int>(state.range(0));
    run_token_benchmark(state, /*winner=*/branches - 1);
}
BENCHMARK(Branch_TokenCancel_LastWinner)
    ->ArgsProduct({{4, 16}, {256}, {16}, {0}})
    ->Iterations(2500)
    ->Unit(benchmark::kMicrosecond);

} // namespace

BENCHMARK_MAIN();
