#include <benchmark/benchmark.h>

#include <atomic>
#include <cstdint>
#include <string>
#include <tbb/task_group.h>

#if defined(__cpp_exceptions) && defined(TBB_USE_EXCEPTIONS) && TBB_USE_EXCEPTIONS
#include <stdexcept>
#endif

#ifndef TBB_EXCEPTION_POLICY_STR
#define TBB_EXCEPTION_POLICY_STR unknown
#endif

#define STR_(x) #x
#define STR(x) STR_(x)

namespace {
const std::string kExceptionPolicy = STR(TBB_EXCEPTION_POLICY_STR);

const bool kContext = []() {
    benchmark::AddCustomContext("backend", "tbb");
    benchmark::AddCustomContext("policy", kExceptionPolicy);
    return true;
}();

void BM_TBB_TaskGroup_NoThrow(benchmark::State& state) {
    const int tasks = static_cast<int>(state.range(0));
    constexpr int kInnerIters = 256;

    for (auto _ : state) {
        std::atomic<std::uint64_t> sum{0};
        tbb::task_group tg;
        for (int t = 0; t < tasks; ++t) {
            tg.run([&, t] {
                std::uint64_t local = static_cast<std::uint64_t>(t + 1);
                for (int i = 0; i < kInnerIters; ++i) {
                    local = local * 2392392u + 1797957575u;
                }
                sum.fetch_add(local, std::memory_order_relaxed);
            });
        }
        tg.wait();
        benchmark::DoNotOptimize(sum.load(std::memory_order_relaxed));
    }

    state.SetItemsProcessed(state.iterations() * tasks);
}

#if defined(__cpp_exceptions) && defined(TBB_USE_EXCEPTIONS) && TBB_USE_EXCEPTIONS
void BM_TBB_TaskGroup_OneThrow(benchmark::State& state) {
    const int tasks = static_cast<int>(state.range(0));

    for (auto _ : state) {
        tbb::task_group tg;
        std::atomic<int> completed{0};
        try {
            for (int t = 0; t < tasks; ++t) {
                tg.run([&, t] {
                    if (t == tasks / 2) {
                        throw std::runtime_error("benchmark throw");
                    }
                    completed.fetch_add(1, std::memory_order_relaxed);
                });
            }
            tg.wait();
        } catch (const std::exception&) {
        }
        benchmark::DoNotOptimize(completed.load(std::memory_order_relaxed));
    }

    state.SetItemsProcessed(state.iterations() * tasks);
}
#endif
} // namespace

BENCHMARK(BM_TBB_TaskGroup_NoThrow)
    ->UseRealTime()
    ->Unit(benchmark::kMicrosecond)
    ->RangeMultiplier(2)
    ->Range(64, 4096);

#if defined(__cpp_exceptions) && defined(TBB_USE_EXCEPTIONS) && TBB_USE_EXCEPTIONS
BENCHMARK(BM_TBB_TaskGroup_OneThrow)
    ->UseRealTime()
    ->Unit(benchmark::kMicrosecond)
    ->RangeMultiplier(2)
    ->Range(64, 4096);
#endif

BENCHMARK_MAIN();
