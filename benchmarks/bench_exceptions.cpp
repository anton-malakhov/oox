#include <string>
#include <benchmark/benchmark.h>

#define STR_(x) #x
#define STR(x) STR_(x)
const std::string parallel_str = STR(PARALLEL);
const std::string policy_str = STR(OOX_EXCEPTION_POLICY_STR);

#include <oox/oox.h>
#if defined(__cpp_exceptions)
#include <exception>
#endif
#include <functional>

namespace {
    const bool kBenchmarkContext = []() {
        benchmark::AddCustomContext("parallel", parallel_str);
        benchmark::AddCustomContext("policy", policy_str);
        return true;
    }();

#if defined(__cpp_exceptions)
    struct dummy_throw : std::exception {
        const char *what() const noexcept override {
            return "dummy throw";
        }
    };
#endif

    const std::plus<int> plus{};

    constexpr int kMaxN = 65536;
    constexpr int kMinIterations = 10;

    void OOX_Single_NoExcept(benchmark::State &state) {
        for (auto _: state) {
            oox::var<int> a = oox::run([]() -> int {
                return 1;
            });
            benchmark::DoNotOptimize(oox::wait_and_get(a));
        }
    }

#if defined(__cpp_exceptions)
    void OOX_Single_Throw(benchmark::State &state) {
        for (auto _: state) {
            oox::var<int> a = oox::run([]() -> int {
                throw dummy_throw{};
            });
            try {
                benchmark::DoNotOptimize(oox::wait_and_get(a));
            } catch (...) {
            }
        }
    }
#endif

    void OOX_Chain_NoExcept(benchmark::State &state) {
        const auto N = static_cast<int>(state.range(0));
        for (auto _: state) {
            oox::var<int> x = oox::run([]() -> int {
                return 1;
            });
            for (int i = 0; i < N; ++i) {
                x = oox::run(plus, 1, x);
            }
            benchmark::DoNotOptimize(oox::wait_and_get(x));
        }
        state.SetItemsProcessed(state.iterations() * (static_cast<int64_t>(N) + 1));
    }

#if defined(__cpp_exceptions)
    void OOX_Chain_RootThrows(benchmark::State &state) {
        const auto N = static_cast<int>(state.range(0));
        for (auto _: state) {
            oox::var<int> x = oox::run([]() -> int {
                throw dummy_throw{};
            });
            for (int i = 0; i < N; ++i) {
                x = oox::run(plus, 1, x);
            }
            try {
                benchmark::DoNotOptimize(oox::wait_and_get(x));
            } catch (...) {
            }
        }
        state.SetItemsProcessed(state.iterations() * (static_cast<int64_t>(N) + 1));
    }
#endif

    void OOX_Diamond_NoExcept(benchmark::State &state) {
        const auto N = static_cast<int>(state.range(0));
        for (auto _: state) {
            oox::var<int> a = oox::run([]() -> int {
                return 1;
            });
            for (int i = 0; i < N; ++i) {
                oox::var<int> b = oox::run(plus, 1, a);
                oox::var<int> c = oox::run(plus, 2, a);
                a = oox::run([](int x, int y) -> int {
                    return x + y;
                }, b, c);
            }
            benchmark::DoNotOptimize(oox::wait_and_get(a));
        }
        state.SetItemsProcessed(state.iterations() * (static_cast<int64_t>(3) * N + 1));
    }

#if defined(__cpp_exceptions)
    void OOX_Diamond_ThrowMiddle(benchmark::State &state) {
        const auto N = static_cast<int>(state.range(0));
        const int throw_at = (N > 0) ? (N / 2) : 0;

        for (auto _: state) {
            oox::var<int> a = oox::run([]() -> int {
                return 1;
            });
            for (int i = 0; i < N; ++i) {
                oox::var<int> b = oox::run(plus, 1, a);
                oox::var<int> c = oox::run(plus, 2, a);

                const bool should_throw = (i == throw_at);
                a = oox::run([should_throw](int x, int y) -> int {
                    if (should_throw) throw dummy_throw{};
                    return x + y;
                }, b, c);
            }

            try {
                benchmark::DoNotOptimize(oox::wait_and_get(a));
            } catch (...) {
            }
        }
        state.SetItemsProcessed(state.iterations() * (3LL * N + 1));
    }
#endif
} // namespace

BENCHMARK(OOX_Single_NoExcept)->UseRealTime()->Unit(benchmark::kNanosecond)->Iterations(kMinIterations)->Arg(8);
#if defined(__cpp_exceptions) && (OOX_DEFAULT_EXCEPTION_POLICY != 0)
BENCHMARK(OOX_Single_Throw)->UseRealTime()->Unit(benchmark::kNanosecond)->Iterations(kMinIterations)->Arg(8);
#endif

BENCHMARK(OOX_Chain_NoExcept)->UseRealTime()->Unit(benchmark::kMicrosecond)->Range(8, kMaxN)->
Iterations(kMinIterations);
#if defined(__cpp_exceptions) && (OOX_DEFAULT_EXCEPTION_POLICY != 0)
BENCHMARK(OOX_Chain_RootThrows)->UseRealTime()->Unit(benchmark::kMicrosecond)->Range(8, kMaxN)->
Iterations(kMinIterations);
#endif

BENCHMARK(OOX_Diamond_NoExcept)->UseRealTime()->Unit(benchmark::kMicrosecond)->Range(8, kMaxN)->
Iterations(kMinIterations);
#if defined(__cpp_exceptions) && (OOX_DEFAULT_EXCEPTION_POLICY != 0)
BENCHMARK(OOX_Diamond_ThrowMiddle)->UseRealTime()->Unit(benchmark::kMicrosecond)->Range(8, kMaxN)->
Iterations(kMinIterations);
#endif

BENCHMARK_MAIN();
