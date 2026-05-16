// Copyright (C) 2021 Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

#define TBB_PREVIEW_TASK_GROUP_EXTENSIONS 1

#undef NDEBUG
#include <benchmark/benchmark.h>
#include <cassert>
#include <string>
#include <oox/oox.h>

#define STR_(x) #x
#define STR(x) STR_(x)
const std::string policy_str = STR(OOX_EXCEPTION_POLICY_STR);

constexpr int FibN = 30;
int cutoff = 8;
constexpr int max_cutoff = 20;
constexpr int cutoff_step = 2;

namespace Serial { // Original problem statement

    int Fib(volatile int n) {
        if(n < 2) return n;
        return Fib(n-1) + Fib(n-2);
    }

}
namespace OOX1 { // Concise 2 lines OOX demonstration

    oox::var<int> Fib(volatile int n) {
        if(n < cutoff) return Serial::Fib(n);
        return oox::run(std::plus<int>(), oox::run(Fib, n-1), oox::run(Fib, n-2) );
    }

}
namespace OOX2 { // Optimized number and order of tasks

    oox::var<int> Fib(volatile int n) {                                         // OOX: High-level continuation style
        if(n < cutoff) return Serial::Fib(n);
        auto right = oox::run(Fib, n-2);                               // spawn right child
        return oox::run(std::plus<int>(), Fib(n-1), std::move(right)); // assign continuation
    }
}

static void Fib_Serial(benchmark::State& state) {
  cutoff = state.range(0);
  for (auto _ : state)
    Serial::Fib(FibN);
}
// Register the function as a benchmark
BENCHMARK(Fib_Serial)->Unit(benchmark::kMillisecond)->UseRealTime()->DenseRange(cutoff, max_cutoff, cutoff_step);

static void Fib_OOX1(benchmark::State& state) {
  cutoff = state.range(0);
  auto fib = Serial::Fib(FibN+cutoff);
  for (auto _ : state) {
    auto x = oox::wait_and_get(OOX1::Fib(FibN+cutoff));
    assert(x == fib);
  }
}
BENCHMARK(Fib_OOX1)->Unit(benchmark::kMillisecond)->UseRealTime()->DenseRange(cutoff, max_cutoff, cutoff_step);

static void Fib_OOX2(benchmark::State& state) {
  cutoff = state.range(0);
  auto fib = Serial::Fib(FibN+cutoff);
  for (auto _ : state) {
    auto x = oox::wait_and_get(OOX2::Fib(FibN+cutoff));
    assert(x == fib);
  }
}
BENCHMARK(Fib_OOX2)->Unit(benchmark::kMillisecond)->UseRealTime()->DenseRange(cutoff, max_cutoff, cutoff_step);

#if HAVE_OMP
namespace OMP {
    int Fib(volatile int n) {
        if(n < cutoff) return Serial::Fib(n);
        int left, right;
        #pragma omp task untied shared(left) firstprivate(n)
        left = Fib(n-1);
        //#pragma omp task untied shared(right) firstprivate(n)
        right = Fib(n-2);
        #pragma omp taskwait
        return left + right;
    }
}

static void Fib_OMP(benchmark::State& state) {
  cutoff = state.range(0);
  for (auto _ : state) {
    #pragma omp parallel
    #pragma omp single
    OMP::Fib(FibN+cutoff);
  }
}
BENCHMARK(Fib_OMP)->Unit(benchmark::kMillisecond)->UseRealTime()->DenseRange(cutoff, max_cutoff, cutoff_step);
#endif

#if HAVE_TBB
#include <tbb/tbb.h>
namespace TBB1 {
    int Fib(volatile int n, tbb::task_group_context &ctx) {                  // TBB: High-level blocking style
        if(n < cutoff) return Serial::Fib(n);
        int left, right;
        tbb::parallel_invoke(
            [&] { left = Fib(n-1, ctx); },
            [&] { right = Fib(n-2, ctx); },
            ctx
        );
        return left + right;
    }
}

static void Fib_TBB1(benchmark::State& state) {
  cutoff = state.range(0);
  for (auto _ : state) {
    tbb::task_group_context ctx;
    TBB1::Fib(FibN+cutoff, ctx);
  }
}
BENCHMARK(Fib_TBB1)->Unit(benchmark::kMillisecond)->UseRealTime()->DenseRange(cutoff, max_cutoff, cutoff_step);

#if TBB_INTERFACE_VERSION >= 12030
namespace TBB2 {
    int Fib(volatile int n, tbb::task_group_context &ctx) {                  // TBB: High-level blocking style
        if(n < cutoff) return Serial::Fib(n);
        int left, right;
        tbb::task_group tg(ctx);
        tg.run( [&] { right = Fib(n-2, ctx); } );
        left = Fib(n-1, ctx);
        tg.wait();
        return left + right;
    }
}

static void Fib_TBB2(benchmark::State& state) {
  cutoff = state.range(0);
  for (auto _ : state) {
    tbb::task_group_context ctx;
    TBB2::Fib(FibN+cutoff, ctx);
  }
}
BENCHMARK(Fib_TBB2)->Unit(benchmark::kMillisecond)->UseRealTime()->DenseRange(cutoff, max_cutoff, cutoff_step);
#endif

#endif //HAVE_TBB

#if HAVE_TF

#include <taskflow/taskflow.hpp>
const int nThreads = std::thread::hardware_concurrency(); // does not respect affinity mask

namespace TF {
    int spawn(volatile int n, tf::Subflow& sbf) {
        if(n < cutoff) return Serial::Fib(n);
        int res1, res2;

        // compute f(n-1)
        sbf.emplace([&res1, n] (tf::Subflow& sbf) { res1 = spawn(n - 1, sbf); } );
            //.name(std::to_string(n-1));

        // compute f(n-2)
        sbf.emplace([&res2, n] (tf::Subflow& sbf) { res2 = spawn(n - 2, sbf); } );
            //.name(std::to_string(n-2));

        sbf.join();
        return res1 + res2;
    }

    void Fib(int N) {
        tf::Executor executor(nThreads);
        tf::Taskflow taskflow("fibonacci");
        int res;  // result
        taskflow.emplace([&res, N] (tf::Subflow& sbf) { 
            res = spawn(N, sbf);  
        }); //.name(std::to_string(N));

        executor.run(taskflow).wait();
    }
}
static void Fib_TF(benchmark::State& state) {
  cutoff = state.range(0);
  for (auto _ : state)
    TF::Fib(FibN+cutoff);
}
BENCHMARK(Fib_TF)->Unit(benchmark::kMillisecond)->UseRealTime()->DenseRange(cutoff, max_cutoff, cutoff_step);
#endif //HAVE_TF

#include <atomic>
#include <memory>

namespace InjectToken {
    using stop_flag = std::shared_ptr<std::atomic<bool>>;
    inline int leaf_or_stop(volatile int n, stop_flag stop) {
        if (stop->load(std::memory_order_relaxed)) return 0;
        return Serial::Fib(n);
    }
    oox::var<int> Fib(volatile int n, stop_flag stop) {
        if (n < cutoff) {
            return oox::run([stop](volatile int nn) {
                return leaf_or_stop(nn, stop);
            }, n);
        }
        if (stop->load(std::memory_order_relaxed)) {
            return oox::run([]() -> int { return 0; });
        }
        auto left  = Fib(n - 1, stop);
        auto right = Fib(n - 2, stop);
        return oox::run(std::plus<int>(), std::move(left), std::move(right));
    }
} // namespace InjectToken

static void Fib_TokenCancel(benchmark::State& state) {
    cutoff = state.range(0);
    for (auto _ : state) {
        auto stop = std::make_shared<std::atomic<bool>>(false);
        auto root = InjectToken::Fib(FibN + cutoff, stop);
        stop->store(true, std::memory_order_relaxed);
        auto v = oox::wait_and_get(root);
        benchmark::DoNotOptimize(v);
    }
}
// Capped iter count for the same reason as Fib_Throw_* / Fib_Cancel_*:
// keeps cumulative wait_and_get count per binary in fib's safe regime.
BENCHMARK(Fib_TokenCancel)
    ->Unit(benchmark::kMillisecond)
    ->UseRealTime()
    ->Iterations(20)
    ->DenseRange(cutoff, max_cutoff, cutoff_step);

#if defined(__cpp_exceptions) && defined(OOX_ENABLE_EXCEPTIONS) && OOX_ENABLE_EXCEPTIONS && \
    defined(OOX_DEFAULT_EXCEPTION_POLICY) && (OOX_DEFAULT_EXCEPTION_POLICY != 0)
#include <exception>

struct dummy_throw_inject : std::exception {
    const char* what() const noexcept override { return "dummy_throw_inject"; }
};

namespace InjectThrow {
    oox::var<int> Fib(volatile int n, int depth, int target_depth, std::atomic<int>* tok) {
        if (n < cutoff) return Serial::Fib(n);
        if (depth == target_depth) {
            int e = 0;
            if (tok->compare_exchange_strong(e, 1,
                                              std::memory_order_relaxed,
                                              std::memory_order_relaxed)) {
                return oox::run([]() -> int { throw dummy_throw_inject{}; });
            }
        }
        auto left  = Fib(n - 1, depth + 1, target_depth, tok);
        auto right = Fib(n - 2, depth + 1, target_depth, tok);
        // Pass by lvalue ref so OOX uses assign_prerequisite (reader path)
        return oox::run(std::plus<int>(), left, right);
    }
} // namespace InjectThrow

static void run_fib_throw(benchmark::State& state, int td) {
    cutoff = state.range(0);
    int caught = 0;
    int fired = 0;
    for (auto _ : state) {
        std::atomic<int> tok{0};
        auto root = InjectThrow::Fib(FibN + cutoff, 0, td, &tok);
        try {
            benchmark::DoNotOptimize(oox::wait_and_get(root));
        } catch (const dummy_throw_inject&) { ++caught; }
        catch (const oox::cancelled_by_exception&) { ++caught; }
        catch (...) { ++caught; }
        fired += tok.load(std::memory_order_relaxed);
    }
    state.counters["caught"] = caught;
    state.counters["fired"] = fired;
    state.counters["target_depth"] = td;
}

static void Fib_Throw_Start (benchmark::State& s) { run_fib_throw(s, 0); }
static void Fib_Throw_Middle(benchmark::State& s) { run_fib_throw(s, FibN / 2); }
static void Fib_Throw_End   (benchmark::State& s) { run_fib_throw(s, (FibN * 4) / 5); }

// Iterations capped on the Throw rows: Fib_Throw_Start spawns a single
// throw task per iter (~6 us), and at --benchmark_min_time=1s Google
// Benchmark would scale that to ~166k iters per sample. The OOX
// scheduler race in notify_successors hangs sporadically in that
// regime (observed at Fib_Throw_Start/18). Cap iters explicitly so the
// row stays well under the race budget regardless of min_time.
BENCHMARK(Fib_Throw_Start) ->Unit(benchmark::kMillisecond)->UseRealTime()->Iterations(20)->DenseRange(cutoff, max_cutoff, cutoff_step);
BENCHMARK(Fib_Throw_Middle)->Unit(benchmark::kMillisecond)->UseRealTime()->Iterations(3) ->DenseRange(cutoff, max_cutoff, cutoff_step);
BENCHMARK(Fib_Throw_End)   ->Unit(benchmark::kMillisecond)->UseRealTime()->Iterations(3) ->DenseRange(cutoff, max_cutoff, cutoff_step);

namespace InjectCancel {
    oox::var<int> Fib(volatile int n, int depth, int target_depth,
                      std::atomic<int>* tok, oox::var<int>& cancel_pred) {
        if (n < cutoff) return Serial::Fib(n);
        if (depth == target_depth) {
            int e = 0;
            if (tok->compare_exchange_strong(e, 1,
                                              std::memory_order_relaxed,
                                              std::memory_order_relaxed)) {
                return oox::run([](int g) { return g + 1; }, cancel_pred);
            }
        }
        auto left  = Fib(n - 1, depth + 1, target_depth, tok, cancel_pred);
        auto right = Fib(n - 2, depth + 1, target_depth, tok, cancel_pred);
        // Pass by lvalue ref so OOX uses the reader path
        return oox::run(std::plus<int>(), left, right);
    }
} // namespace InjectCancel

static void run_fib_cancel(benchmark::State& state, int td) {
    cutoff = state.range(0);
    int caught = 0;
    int fired = 0;
    for (auto _ : state) {
        oox::var<int> gate(oox::deferred);
        oox::var<int> cancel_pred = oox::run([](int g) { return g + 1; }, gate);
        std::atomic<int> tok{0};
        auto root = InjectCancel::Fib(FibN + cutoff, 0, td, &tok, cancel_pred);
        cancel_pred.cancel();
        oox::run([](int& g) { g = 1; }, gate);
        try {
            benchmark::DoNotOptimize(oox::wait_and_get(root));
        } catch (const oox::cancelled_by_user&) { ++caught; }
        catch (const oox::cancelled_by_exception&) { ++caught; }
        catch (...) { ++caught; }
        fired += tok.load(std::memory_order_relaxed);
    }
    state.counters["caught"] = caught;
    state.counters["fired"] = fired;
    state.counters["target_depth"] = td;
}

static void Fib_Cancel_Start (benchmark::State& s) { run_fib_cancel(s, 0); }
static void Fib_Cancel_Middle(benchmark::State& s) { run_fib_cancel(s, FibN / 2); }
static void Fib_Cancel_End   (benchmark::State& s) { run_fib_cancel(s, (FibN * 4) / 5); }

// Same cap rationale as Fib_Throw_* — Fib_Cancel_Start is a single
// gated cancel_pred task per iter that GB would scale to many tens of
// thousands at min_time=1s.
BENCHMARK(Fib_Cancel_Start) ->Unit(benchmark::kMillisecond)->UseRealTime()->Iterations(20)->DenseRange(cutoff, max_cutoff, cutoff_step);
BENCHMARK(Fib_Cancel_Middle)->Unit(benchmark::kMillisecond)->UseRealTime()->Iterations(3) ->DenseRange(cutoff, max_cutoff, cutoff_step);
BENCHMARK(Fib_Cancel_End)   ->Unit(benchmark::kMillisecond)->UseRealTime()->Iterations(3) ->DenseRange(cutoff, max_cutoff, cutoff_step);

#endif // OOX_ENABLE_EXCEPTIONS && OOX_DEFAULT_EXCEPTION_POLICY

namespace {
const bool kBenchmarkContext = []() {
    benchmark::AddCustomContext("policy", policy_str);
    return true;
}();
}

BENCHMARK_MAIN();
