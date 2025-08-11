#define TBB_PREVIEW_TASK_GROUP_EXTENSIONS 1

#include "../oox/oox.h"
#include <benchmark/benchmark.h>
#include <vector>

constexpr int num_threads = 5;
constexpr int num_parts = 20;
constexpr int element = 5;
constexpr int size = 1234567;
constexpr int pos_elem = 436587;
std::vector<int> nums = [] { std::vector<int> tmp(size, 0); tmp[pos_elem] = element; return tmp; }();

int DefaultLinSearchPart(int l_ind, int r_ind, int elem) {
    int index = -1;
    while(l_ind < r_ind) {
        if (nums[l_ind] == elem) {
          return l_ind;
        }
        l_ind++;
    }
    return index;
}

static void SerialLinSearch(benchmark::State& state) {
  for (auto _ : state) {
    benchmark::DoNotOptimize(DefaultLinSearchPart(0, size, element));
  }
}
// Register the function as a benchmark
BENCHMARK(SerialLinSearch)->Unit(benchmark::kMillisecond)->UseRealTime();

#if HAVE_TBB
#include <tbb/tbb.h>

int DefaultLinSearch(int n_th, int n_pr, int elem, bool use_cancellation) {
    tbb::global_control global_limit(tbb::global_control::max_allowed_parallelism, num_threads); 
    std::vector<oox::var<int>> workers;
    int index = -1;
    int len = size / num_parts;
    int l_ind = 0;
    for (; l_ind < size - len; l_ind += len) {
        workers.emplace_back(oox::run(DefaultLinSearchPart, l_ind, l_ind + len, elem)); 
    }
    workers.emplace_back(oox::run(DefaultLinSearchPart, l_ind, size, elem));

    for (auto& worker : workers) {
        auto tmp = oox::wait_optional(worker);
        if (tmp && tmp.value() != -1 && index == -1) {
            index = tmp.value();
            if (use_cancellation) {
              oox::cancel();
            }
        }
    }
    oox::reset();
    return index;
}

static void ParallelLinSearch(benchmark::State& state) {
  for (auto _ : state) {
    DefaultLinSearch(num_threads, num_parts, element, false);
  }
}

BENCHMARK(ParallelLinSearch)->Unit(benchmark::kMillisecond)->UseRealTime();

static void ParallelLinSearchWithCancellation(benchmark::State& state) {
  for (auto _ : state) {
    DefaultLinSearch(num_threads, num_parts, element, true);
  }
}

BENCHMARK(ParallelLinSearchWithCancellation)->Unit(benchmark::kMillisecond)->UseRealTime();

#endif //HAVE_TBB

BENCHMARK_MAIN();
