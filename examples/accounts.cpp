// Copyright (C) 2026
//
// SPDX-License-Identifier: Apache-2.0

// Accounts demo for oox::shared_var<T>.
//
// Accounts are stored as an array of oox::shared_var<account>. Points are
// transferred between two random accounts 100 000 times. Every transfer is an
// OOX task that writes both accounts; each shared_var serializes the writers
// chained onto it across all threads, so no points are lost or duplicated.
//
// Invariant checked at the end: the total number of points is conserved.
//
// Note: a transfer is not atomic as a whole — an observer could see a state
// where one account was debited before the other is credited. Per-account
// serialization is guaranteed; cross-account atomicity would need a separate
// synchronization layer (e.g. a global ordering task).
//
// Build & run (with a thread pool backend like TBB for real parallelism):
//   cmake -B build -S . && cmake --build build --target accounts_example
//   ./build/examples/accounts_example [num_accounts] [num_transfers]

#include <oox/shared_var.h>

#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <vector>

namespace {

struct account {
    int points;

    // shared_var writers/readers may lazily materialize a default value,
    // so account must be default-constructible (same requirement as oox::var).
    account() noexcept : points(0) {}
    explicit account(int p) noexcept : points(p) {}
};

// Deterministic LCG so the demo is reproducible.
std::uint32_t lcg_state = 0x12345678u;
std::uint32_t lcg() {
    lcg_state = lcg_state * 1664525u + 1013904223u;
    return lcg_state;
}

} // namespace

int main(int argc, char** argv) {
    const int num_accounts = (argc > 1) ? std::atoi(argv[1]) : 10;
    const int num_transfers = (argc > 2) ? std::atoi(argv[2]) : 100000;
    const int initial_points = 1000;

    std::printf("accounts: %d, transfers: %d\n", num_accounts, num_transfers);

    std::vector<oox::shared_var<account>> accounts;
    accounts.reserve(num_accounts);
    for (int i = 0; i < num_accounts; ++i) {
        accounts.emplace_back(account(initial_points));
    }

    const long long initial_total =
        static_cast<long long>(num_accounts) * initial_points;

    // Register the transfers. Each transfer is one task writing both accounts;
    // the result var is dropped immediately (the task stays alive through its
    // own lifetime references, as usual for oox::run).
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

    // Wait for every account's last transfer and check conservation of points.
    long long total = 0;
    int min_balance = initial_points;
    int max_balance = 0;
    for (auto& acc : accounts) {
        const account a = oox::wait_and_get(acc);
        total += a.points;
        if (a.points < min_balance) {
            min_balance = a.points;
        }
        if (a.points > max_balance) {
            max_balance = a.points;
        }
    }

    std::printf("total points: %lld (expected %lld)\n", total, initial_total);
    std::printf("min/max balance: %d / %d\n", min_balance, max_balance);

    if (total != initial_total) {
        std::fprintf(stderr, "FAIL: points were lost or created during transfers\n");
        return 1;
    }

    std::printf("OK: points conserved across %d transfers\n", num_transfers);
    return 0;
}
