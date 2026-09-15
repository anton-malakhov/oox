#pragma once

#include <cstddef>
#include <cstdlib>
#include <cstdint>
#include <utility>

#include <fmt/core.h>

#include <twist/build.hpp>
#if __TWIST_SIM__
#include <twist/sim.hpp>
#endif

#ifndef OOX_USING_TWIST
#error "Twist tests must compile OOX with HAVE_TWIST / OOX_USING_TWIST"
#endif

#ifndef OOX_TWIST_RANDOM_SEEDS
#define OOX_TWIST_RANDOM_SEEDS 32
#endif

#ifndef OOX_TWIST_MAX_STEPS
#define OOX_TWIST_MAX_STEPS 10000
#endif

#ifndef OOX_TWIST_MAX_PREEMPTS
#define OOX_TWIST_MAX_PREEMPTS 3
#endif

namespace oox::twist_tests {

template <typename Scenario>
auto DrainSpawnedTasks(Scenario scenario) {
    return [scenario = std::move(scenario)]() mutable {
        internal::twist_task_tracking_scope tracking;
        scenario();
        tracking.drain();
    };
}

#if __TWIST_SIM__

inline void ReportFailure(const char* scheduler,
                          const char* scenario,
                          std::uint64_t seed,
                          const twist::sim::Result& result) {
    fmt::print(stderr,
               "Twist scenario '{}' failed under {} seed/bound {}\n"
               "status: {}\n"
               "stderr:\n{}\n",
               scenario,
               scheduler,
               seed,
               static_cast<int>(result.status),
               result.std_err);
}

template <typename Scenario>
void RunRandomSeeds(const char* name, Scenario scenario, std::size_t seeds = OOX_TWIST_RANDOM_SEEDS) {
    static_assert(twist::build::kTwisted, "Twist tests must use a twisted runtime build");

    for (std::uint64_t seed = 1; seed <= seeds; ++seed) {
        twist::sim::sched::RandomScheduler scheduler{{.seed = seed}};
        twist::sim::Simulator simulator{&scheduler};

        auto result = simulator.Run(DrainSpawnedTasks(scenario));
        if (!result.Ok()) {
            ReportFailure("RandomScheduler", name, seed, result);
            std::abort();
        }
    }
}

template <typename Scenario>
void RunDfs(const char* name, Scenario scenario) {
    static_assert(twist::build::kTwisted, "Twist tests must use a twisted runtime build");

    twist::sim::sched::DfsScheduler dfs{{
        .max_preempts = OOX_TWIST_MAX_PREEMPTS,
        .max_steps = OOX_TWIST_MAX_STEPS,
    }};

    auto tracked_scenario = DrainSpawnedTasks(scenario);
    auto exploration = twist::sim::Explore(dfs, tracked_scenario);
    if (exploration.found) {
        const auto& found = *exploration.found;
        ReportFailure("DfsScheduler", name, OOX_TWIST_MAX_PREEMPTS, found.result);
        twist::sim::Print(tracked_scenario, found.schedule);
        std::abort();
    }
}

#else

template <typename Scenario>
void RunRandomSeeds(const char* /*name*/, Scenario scenario, std::size_t seeds = OOX_TWIST_RANDOM_SEEDS) {
    static_assert(twist::build::kTwisted, "Twist tests must use a twisted runtime build");

    for (std::uint64_t seed = 1; seed <= seeds; ++seed) {
        (void)seed;
        DrainSpawnedTasks(scenario)();
    }
}

template <typename Scenario>
void RunDfs(const char* name, Scenario scenario) {
    RunRandomSeeds(name, scenario, 1);
}

#endif

} // namespace oox::twist_tests
