#include <atomic>
#include <cstdint>
#include <string>
#include <vector>

#include <benchmark/benchmark.h>

#include <oox/oox.h>
#include <tbb/task_group.h>

#if defined(__SANITIZE_THREAD__)
#define OOX_BENCHMARK_TSAN 1
#elif defined(__has_feature)
#if __has_feature(thread_sanitizer)
#define OOX_BENCHMARK_TSAN 1
#endif
#endif
#ifndef OOX_BENCHMARK_TSAN
#define OOX_BENCHMARK_TSAN 0
#endif

namespace {
constexpr int kSplitDepth = 8;
constexpr int kMinDepth = 14;
constexpr int kMaxDepth = 22;
constexpr int kDepthStep = 2;
constexpr int kLeafWork = 64;
constexpr double kBenchmarkMinTime = 1.0;

struct SearchConfig {
    int depth;
    int split_depth;
    int branch_count;
    int leaves_per_branch;
    int target;
};

SearchConfig make_config(int depth) {
    const int split_depth = depth < kSplitDepth ? depth : kSplitDepth;
    const int branch_count = 1 << split_depth;
    const int leaves_per_branch = 1 << (depth - split_depth);
    return {depth, split_depth, branch_count, leaves_per_branch, leaves_per_branch / 2};
}

struct found_by_exception {};

std::uint64_t leaf_work(int leaf, int work) {
    std::uint64_t x = static_cast<std::uint64_t>(leaf + 1);
    for (int i = 0; i < work; ++i) {
        x = x * 6364136223846793005ULL + 1442695040888963407ULL;
    }
    return x;
}

bool search_branch(int first_leaf,
                   int leaf_count,
                   int target,
                   std::atomic<int>& found,
                   std::atomic<std::int64_t>& visited) {
    for (int offset = 0; offset < leaf_count; ++offset) {
        if (found.load(std::memory_order_acquire) >= 0) {
            return false;
        }

        const int leaf = first_leaf + offset;
        benchmark::DoNotOptimize(leaf_work(leaf, kLeafWork));
        visited.fetch_add(1, std::memory_order_relaxed);

        if (leaf == target) {
            int expected = -1;
            return found.compare_exchange_strong(expected, leaf, std::memory_order_acq_rel);
        }
    }
    return false;
}

void search_branch_throw(int first_leaf,
                         int leaf_count,
                         int target,
                         std::atomic<int>& found,
                         std::atomic<std::int64_t>& visited) {
    for (int offset = 0; offset < leaf_count; ++offset) {
        if (found.load(std::memory_order_acquire) >= 0) {
            return;
        }

        const int leaf = first_leaf + offset;
        benchmark::DoNotOptimize(leaf_work(leaf, kLeafWork));
        visited.fetch_add(1, std::memory_order_relaxed);

        if (leaf == target) {
            int expected = -1;
            if (found.compare_exchange_strong(expected, leaf, std::memory_order_acq_rel)) {
                throw found_by_exception{};
            }
            return;
        }
    }
}

void BM_TBB_BranchingSearchCancel(benchmark::State& state) {
    const SearchConfig cfg = make_config(static_cast<int>(state.range(0)));
    std::int64_t total_visited = 0;

    for (auto _ : state) {
        std::atomic<int> found{-1};
        std::atomic<std::int64_t> visited{0};
        tbb::task_group_context ctx;
        tbb::task_group tg(ctx);

        for (int branch = 0; branch < cfg.branch_count; ++branch) {
            tg.run([cfg, branch, &found, &visited, &ctx] {
                const int first_leaf = branch * cfg.leaves_per_branch;
                if (search_branch(first_leaf, cfg.leaves_per_branch, cfg.target, found, visited)) {
                    ctx.cancel_group_execution();
                }
            });
        }

        tg.wait();
        benchmark::DoNotOptimize(found.load(std::memory_order_acquire));
        total_visited += visited.load(std::memory_order_relaxed);
    }

    state.SetItemsProcessed(total_visited);
}

void BM_TBB_BranchingSearchThrow(benchmark::State& state) {
    const SearchConfig cfg = make_config(static_cast<int>(state.range(0)));
    std::int64_t total_visited = 0;

    for (auto _ : state) {
        std::atomic<int> found{-1};
        std::atomic<std::int64_t> visited{0};
        tbb::task_group tg;

        for (int branch = 0; branch < cfg.branch_count; ++branch) {
            tg.run([cfg, branch, &found, &visited] {
                const int first_leaf = branch * cfg.leaves_per_branch;
                search_branch_throw(first_leaf, cfg.leaves_per_branch, cfg.target, found, visited);
            });
        }

        try {
            tg.wait();
        } catch (const found_by_exception&) {
        }
        benchmark::DoNotOptimize(found.load(std::memory_order_acquire));
        total_visited += visited.load(std::memory_order_relaxed);
    }

    state.SetItemsProcessed(total_visited);
}

void BM_OOX_BranchingSearchCancel(benchmark::State& state) {
    const SearchConfig cfg = make_config(static_cast<int>(state.range(0)));
    std::int64_t total_visited = 0;

    for (auto _ : state) {
        std::atomic<int> found{-1};
        std::atomic<std::int64_t> visited{0};
        oox::var<int> gate(oox::deferred);
        std::vector<oox::var<int>> branches;
        branches.reserve(static_cast<std::size_t>(cfg.branch_count));

        for (int branch = 0; branch < cfg.branch_count; ++branch) {
            branches.push_back(oox::run([&, branch](int) -> int {
                const int first_leaf = branch * cfg.leaves_per_branch;
                if (search_branch(first_leaf, cfg.leaves_per_branch, cfg.target, found, visited)) {
                    for (int i = 0; i < cfg.branch_count; ++i) {
                        if (i != branch) {
                            branches[static_cast<std::size_t>(i)].cancel();
                        }
                    }
                    return cfg.target;
                }
                return -1;
            }, gate));
        }

        oox::run([](int& g) { g = 1; }, gate);
        for (auto& branch : branches) {
            benchmark::DoNotOptimize(oox::wait_for_all_status<false>(branch));
        }
        benchmark::DoNotOptimize(found.load(std::memory_order_acquire));
        total_visited += visited.load(std::memory_order_relaxed);
    }

    state.SetItemsProcessed(total_visited);
}

void BM_OOX_BranchingSearchThrow(benchmark::State& state) {
    const SearchConfig cfg = make_config(static_cast<int>(state.range(0)));
    std::int64_t total_visited = 0;

    for (auto _ : state) {
        std::atomic<int> found{-1};
        std::atomic<std::int64_t> visited{0};
        oox::var<int> gate(oox::deferred);
        std::vector<oox::var<int>> branches;
        branches.reserve(static_cast<std::size_t>(cfg.branch_count));

        for (int branch = 0; branch < cfg.branch_count; ++branch) {
            branches.push_back(oox::run([&, branch](int) -> int {
                const int first_leaf = branch * cfg.leaves_per_branch;
                search_branch_throw(first_leaf, cfg.leaves_per_branch, cfg.target, found, visited);
                return -1;
            }, gate));
        }

        oox::run([](int& g) { g = 1; }, gate);
        for (auto& branch : branches) {
            try {
                benchmark::DoNotOptimize(oox::wait_and_get(branch));
            } catch (const found_by_exception&) {
            }
        }
        benchmark::DoNotOptimize(found.load(std::memory_order_acquire));
        total_visited += visited.load(std::memory_order_relaxed);
    }

    state.SetItemsProcessed(total_visited);
}
} // namespace

#if !OOX_BENCHMARK_TSAN
BENCHMARK(BM_TBB_BranchingSearchCancel)
    ->UseRealTime()
    ->Unit(benchmark::kMicrosecond)
    ->DenseRange(kMinDepth, kMaxDepth, kDepthStep)
    ->MinTime(kBenchmarkMinTime);

BENCHMARK(BM_TBB_BranchingSearchThrow)
    ->UseRealTime()
    ->Unit(benchmark::kMicrosecond)
    ->DenseRange(kMinDepth, kMaxDepth, kDepthStep)
    ->MinTime(kBenchmarkMinTime);
#endif

BENCHMARK(BM_OOX_BranchingSearchCancel)
    ->UseRealTime()
    ->Unit(benchmark::kMicrosecond)
    ->DenseRange(kMinDepth, kMaxDepth, kDepthStep)
    ->MinTime(kBenchmarkMinTime);

BENCHMARK(BM_OOX_BranchingSearchThrow)
    ->UseRealTime()
    ->Unit(benchmark::kMicrosecond)
    ->DenseRange(kMinDepth, kMaxDepth, kDepthStep)
    ->MinTime(kBenchmarkMinTime);

namespace {
const bool kBenchmarkContext = []() {
    benchmark::AddCustomContext("benchmark", "branching_cancel");
    benchmark::AddCustomContext("policy", "exc");
    benchmark::AddCustomContext("split_depth", std::to_string(kSplitDepth));
    benchmark::AddCustomContext("leaf_work", std::to_string(kLeafWork));
    return true;
}();
}

BENCHMARK_MAIN();
