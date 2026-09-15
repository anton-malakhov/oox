// Copyright (C) 2026
//
// SPDX-License-Identifier: Apache-2.0

// Benchmark: point transfers between accounts stored as an array of
// oox::shared_var<account> (same pattern as examples/accounts.cpp).
//
// Each iteration:
//   1. creates `num_accounts` shared_var<account> handles;
//   2. registers `num_transfers` random transfers (one task writes both
//      accounts; per-account writer chains serialize concurrent updates);
//   3. waits for every account's last transfer.
//
// Reported metric: wall time per iteration. Throughput is
// num_transfers / Time (the framework's items-per-second counters divide by
// the whole run duration, which would understate the per-iteration rate).

#include <benchmark/benchmark.h>
#include <oox/shared_var.h>

#include <cstdint>
#include <vector>

namespace {

struct account {
    int points;

    // shared_var writers/readers may lazily materialize a default value,
    // so account must be default-constructible (same requirement as oox::var).
    account() noexcept : points(0) {}
    explicit account(int p) noexcept : points(p) {}
};

// Deterministic LCG, reset per iteration so every iteration does the same work.
std::uint32_t lcg_state = 0x12345678u;
std::uint32_t lcg() {
    lcg_state = lcg_state * 1664525u + 1013904223u;
    return lcg_state;
}

void BenchAccounts(benchmark::State& state) {
    const int num_accounts = static_cast<int>(state.range(0));
    const int num_transfers = static_cast<int>(state.range(1));
    const int initial_points = 1000;

    for (auto _ : state) {
        std::vector<oox::shared_var<account>> accounts;
        accounts.reserve(num_accounts);
        for (int i = 0; i < num_accounts; ++i) {
            accounts.emplace_back(account(initial_points));
        }

        lcg_state = 0x12345678u;
        for (int i = 0; i < num_transfers; ++i) {
            const int from = lcg() % num_accounts;
            const int to = lcg() % num_accounts;
            if (from == to) {
                continue; // no self-transfers
            }
            const int amount = 1 + static_cast<int>(lcg() % 10);
            oox::run([from, to, amount](account& a, account& b) {
                a.points -= amount;
                b.points += amount;
            }, accounts[from], accounts[to]);
        }

        // Wait for every account's last transfer; per-account chains then
        // guarantee all transfers have completed.
        for (auto& acc : accounts) {
            benchmark::DoNotOptimize(oox::wait_and_get(acc).points);
        }
    }
}

} // namespace

BENCHMARK(BenchAccounts)
    ->Args({10, 100000})
    ->Args({100, 100000})
    ->Args({10, 1000000})
    ->Unit(benchmark::kMillisecond);

BENCHMARK_MAIN();
