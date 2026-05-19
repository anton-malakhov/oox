// Shared benchmark utilities for the OOX benchmark suite. This header is
// intentionally branch-agnostic: it defines a benchmark-local cooperative
// cancellation token/source and a deterministic CPU work helper. Branch
// specific adapters live in the per-branch benchmark files themselves; this
// header has no dependency on <oox/oox.h>.

#ifndef OOX_BENCHMARKS_BENCH_COMMON_HPP
#define OOX_BENCHMARKS_BENCH_COMMON_HPP

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <utility>

#include <benchmark/benchmark.h>

namespace bench_oox {

struct cancellation_state {
    std::atomic<bool> requested{false};
};

class cancellation_token {
public:
    cancellation_token() = default;
    explicit cancellation_token(std::shared_ptr<cancellation_state> state)
        : state_(std::move(state)) {}

    bool stop_requested() const noexcept {
        return state_ && state_->requested.load(std::memory_order_relaxed);
    }

private:
    std::shared_ptr<cancellation_state> state_;
};

class cancellation_source {
public:
    cancellation_source()
        : state_(std::make_shared<cancellation_state>()) {}

    cancellation_token token() const noexcept {
        return cancellation_token{state_};
    }

    bool request_cancel() noexcept {
        bool expected = false;
        return state_->requested.compare_exchange_strong(
            expected, true,
            std::memory_order_relaxed,
            std::memory_order_relaxed);
    }

    bool stop_requested() const noexcept {
        return state_->requested.load(std::memory_order_relaxed);
    }

private:
    std::shared_ptr<cancellation_state> state_;
};


inline std::uint64_t spin_work(std::uint64_t seed, std::size_t iterations) {
    std::uint64_t x = seed + 0x9e3779b97f4a7c15ull;
    for (std::size_t i = 0; i < iterations; ++i) {
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        x *= 0x2545f4914f6cdd1dull;
        benchmark::DoNotOptimize(x);
    }
    return x;
}

} // namespace bench_oox

#endif // OOX_BENCHMARKS_BENCH_COMMON_HPP
