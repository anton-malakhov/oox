// SPDX-License-Identifier: Apache-2.0
#pragma once
#include "oox/eigen/rapid_auto.h"
#include "oox/eigen/rapid_parallel_for.h"
#include <string_view>

namespace rapid_group_experiment {
namespace eigen = oox::detail::eigen_pool;
namespace rapid = eigen::rapid;

enum class Policy { Lazy, Auto, Simple, Static, Affinity };
enum class Placement { Local, All, Outsiders };
struct Options {
  unsigned workers = 8, group = 8, residents = 8, minimum_ms = 40;
  Policy policy = Policy::Auto;
  Placement placement = Placement::Local;
  bool diagnose = false;
};
std::string_view Name(Policy value);
std::string_view Name(Placement value);
Options Parse(int argc, char **argv);
void Validate(const Options &options);
unsigned Target(Placement placement, unsigned workers, unsigned group,
                unsigned publisher, size_t sequence);

// A benchmark-only override: normal runtime headers and their defaults are
// unchanged. Native Auto/Simple reach Schedule through Region's pool reference.
class Pool final : public eigen::ThreadPool {
public:
  struct Counters { size_t group_publications = 0, routed = 0, routed_outside = 0; };
  explicit Pool(const Options &options);
  void Schedule(eigen::Task *task) override;
  Counters Snapshot() const;
private:
  struct alignas(128) Cursor {
    size_t next = 0;
#ifdef OOX_GROUP_DIAGNOSTICS
    Counters counters;
#endif
  };
  const Placement placement_;
  const unsigned group_;
  std::unique_ptr<Cursor[]> cursors_;
};

class Runtime {
public:
  explicit Runtime(const Options &options);
  void Prepare();
  Pool &Executor() { return pool_; }
  const Options &Config() const { return options_; }
  template <typename F>
  size_t Run(size_t first, size_t last, F &&body, size_t grain = 2) {
    size_t activated = 0;
    switch (options_.policy) {
    case Policy::Lazy:
      rapid::ParallelForAuto(group_, first, last, std::forward<F>(body), grain, nullptr, &activated);
      break;
    case Policy::Auto:
      rapid::ParallelFor(group_, first, last, std::forward<F>(body), eigen::AutoPartitioner{}, grain, nullptr, &activated);
      break;
    case Policy::Simple:
      rapid::ParallelFor(group_, first, last, std::forward<F>(body), eigen::SimplePartitioner{}, grain, nullptr, &activated);
      break;
    case Policy::Static:
      rapid::ParallelFor(group_, first, last, std::forward<F>(body), eigen::StaticPartitioner{}, grain, nullptr, &activated);
      break;
    case Policy::Affinity:
      rapid::ParallelFor(group_, first, last, std::forward<F>(body), affinity_, grain, nullptr, &activated);
      break;
    }
    return activated;
  }
private:
  const Options options_;
  Pool pool_;
  rapid::RapidDomainState domain_;
  rapid::RapidStartGroup group_;
  eigen::AffinityPartitioner affinity_;
};
} // namespace rapid_group_experiment
