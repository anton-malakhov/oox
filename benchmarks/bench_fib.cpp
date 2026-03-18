// Copyright (C) 2021 Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

#define TBB_PREVIEW_TASK_GROUP_EXTENSIONS 1

#undef NDEBUG
#include <benchmark/benchmark.h>
#include <cassert>
#include <oox/oox.h>

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

BENCHMARK_MAIN();
