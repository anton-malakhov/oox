// Copyright (C) 2026
//
// SPDX-License-Identifier: Apache-2.0

// Benchmark: the "get-heavy" pattern — readers call get() on one shared_var
// while writers keep registering tasks with a non-trivial body.
//
// This is the pattern where variant 1's design may hurt: get()/wait() hold
// the state mutex for the whole blocking wait, so every writer registration
// on the same shared_var stalls until the readers' waits finish.
//
// Split: half the threads are writers (register a compute task), half are
// readers (block on get()). Metric: total operations (gets + registrations)
// per second.

#include <benchmark/benchmark.h>
#include <oox/shared_var.h>

#include <cstdint>

namespace {

// Task body size: the readers' waits are roughly kComputeIters * ~2ns.
// The recurrence acc = acc*31 + i is deliberately not optimizable to a closed
// form (a plain sum of squares gets folded by -O3, making the tasks ~free).
constexpr int kComputeIters = 50000;

void BenchGetHeavy(benchmark::State& state) {
    oox::shared_var<int> value(0);
    const int id = static_cast<int>(state.thread_index());
    const int threads = static_cast<int>(state.threads());
    const bool is_writer = id < threads / 2;

    for (auto _ : state) {
        if (is_writer) {
            // Register a writer task with a small compute body; the result
            // var is dropped immediately (the task stays alive as usual).
            oox::run([id](int& v) {
                int acc = id;
                for (int i = 0; i < kComputeIters; ++i) {
                    acc = acc * 31 + i;
                }
                benchmark::DoNotOptimize(acc);
                v = acc;
            }, value);
        } else {
            // Read the current value; blocks until the current writer task
            // completes. Variant 1 holds the state mutex during this wait.
            benchmark::DoNotOptimize(value.get());
        }
        state.SetItemsProcessed(1);
    }
}

} // namespace

BENCHMARK(BenchGetHeavy)->ThreadRange(2, 16)->UseRealTime();

BENCHMARK_MAIN();
