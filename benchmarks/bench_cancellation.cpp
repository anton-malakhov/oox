#include <string>
#include <benchmark/benchmark.h>

#define STR_(x) #x
#define STR(x) STR_(x)
const std::string parallel_str = STR(PARALLEL);
const std::string policy_str = STR(OOX_EXCEPTION_POLICY_STR);

#include <oox/oox.h>

#if defined(__cpp_exceptions) && defined(OOX_ENABLE_EXCEPTIONS) && OOX_ENABLE_EXCEPTIONS && \
    defined(OOX_DEFAULT_EXCEPTION_POLICY) && OOX_DEFAULT_EXCEPTION_POLICY
#define OOX_BENCH_CANCELLATION 1
#include <vector>
#else
#define OOX_BENCH_CANCELLATION 0
#endif

namespace {
#if OOX_BENCH_CANCELLATION
    const auto plus = [](int x, int y) -> int {
        return x + y;
    };
    constexpr int kMaxN = 64;
    constexpr int kMinIterations = 10;

    bool wait_cancelled_by_user(oox::var<int>& value) {
        try {
            benchmark::DoNotOptimize(oox::wait_and_get(value));
        } catch (const oox::cancelled_by_user&) {
            return true;
        }
        return false;
    }

    void satisfy_gate(oox::var<int>& gate) {
        oox::run([](int& g) { g = 1; }, gate);
    }

    std::vector<oox::var<int>> make_fanout(oox::var<int>& input, int N) {
        std::vector<oox::var<int>> leaves;
        leaves.reserve(static_cast<std::size_t>(N));
        for (int i = 0; i < N; ++i) {
            leaves.push_back(oox::run(plus, i, input));
        }
        return leaves;
    }

    bool wait_all_cancelled(std::vector<oox::var<int>>& values) {
        bool all_cancelled = true;
        for (auto& value : values) {
            all_cancelled = wait_cancelled_by_user(value) && all_cancelled;
        }
        return all_cancelled;
    }

    void OOX_Cancel_Chain(benchmark::State& state) {
        const auto N = static_cast<int>(state.range(0));

        for (auto _ : state) {
            oox::var<int> gate(oox::deferred);
            oox::var<int> cancelled = oox::run([](int x) -> int {
                return x + 1;
            }, gate);
            oox::var<int> tail = oox::run(plus, 1, cancelled);

            for (int i = 1; i < N; ++i) {
                tail = oox::run(plus, 1, tail);
            }

            cancelled.cancel();
            satisfy_gate(gate);
            if (!wait_cancelled_by_user(tail)) {
                state.SkipWithError("chain tail was not cancelled by user");
                break;
            }
        }

        state.SetItemsProcessed(state.iterations() * (static_cast<int64_t>(N) + 1));
    }

    void OOX_Cancel_Fanout(benchmark::State& state) {
        const auto N = static_cast<int>(state.range(0));

        for (auto _ : state) {
            oox::var<int> gate(oox::deferred);
            oox::var<int> cancelled = oox::run([](int x) -> int {
                return x + 1;
            }, gate);
            auto leaves = make_fanout(cancelled, N);

            cancelled.cancel();
            satisfy_gate(gate);

            if (!wait_all_cancelled(leaves)) {
                state.SkipWithError("fanout leaf was not cancelled by user");
                break;
            }
        }

        state.SetItemsProcessed(state.iterations() * (static_cast<int64_t>(N) + 1));
    }
#endif
} // namespace

#if OOX_BENCH_CANCELLATION
BENCHMARK(OOX_Cancel_Chain)->UseRealTime()->Unit(benchmark::kMicrosecond)->Range(8, kMaxN)->Iterations(2500);
BENCHMARK(OOX_Cancel_Fanout)->UseRealTime()->Unit(benchmark::kMicrosecond)->Range(8, kMaxN)->Iterations(2500);
#endif

namespace {
const bool kBenchmarkContext = []() {
    benchmark::AddCustomContext("parallel", parallel_str);
    benchmark::AddCustomContext("policy", policy_str);
    return true;
}();
}

BENCHMARK_MAIN();
