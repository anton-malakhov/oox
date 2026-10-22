// SPDX-License-Identifier: Apache-2.0
#include "experiment.h"
#include "benchmarks/eigen/arguments.h"
#include "test_support/eigen_resident.h"

namespace rapid_group_experiment {
std::string_view Name(Policy value) {
  switch (value) {
  case Policy::Lazy: return "lazy";
  case Policy::Auto: return "auto";
  case Policy::Simple: return "simple";
  case Policy::Static: return "static";
  case Policy::Affinity: return "affinity";
  }
  throw std::invalid_argument("invalid policy");
}
std::string_view Name(Placement value) {
  switch (value) {
  case Placement::Local: return "local";
  case Placement::All: return "all";
  case Placement::Outsiders: return "outsiders";
  }
  throw std::invalid_argument("invalid placement");
}

void Validate(const Options &o) {
  if (!o.workers || o.workers > 1024 || !o.group || o.group > std::min(o.workers, 64u) ||
      o.residents > o.workers || o.minimum_ms < 5 || o.minimum_ms > 1000)
    throw std::invalid_argument("workers=1..1024; group=1..min(workers,64); residents=0..workers; min-ms=5..1000");
  if (o.placement != Placement::Local && o.policy != Policy::Auto && o.policy != Policy::Simple)
    throw std::invalid_argument("placement overrides apply only to native auto/simple; lazy/static/affinity retain their semantics");
  if (o.placement == Placement::Outsiders && o.group == o.workers)
    throw std::invalid_argument("outsiders placement requires group < workers");
}

Options Parse(int argc, char **argv) {
  Options o;
  bool group_set = false, residents_set = false;
  for (int i = 1; i < argc; ++i) {
    const std::string_view key = argv[i];
    if (key == "--diagnose") { o.diagnose = true; continue; }
    if (++i == argc) throw std::invalid_argument("missing option value");
    const std::string_view value = argv[i];
    if (key == "--workers") o.workers = benchmark_arguments::Number<unsigned>(value, key, 1, 1024);
    else if (key == "--group") { o.group = benchmark_arguments::Number<unsigned>(value, key, 1, 64); group_set = true; }
    else if (key == "--residents") { o.residents = benchmark_arguments::Number<unsigned>(value, key, 0, 1024); residents_set = true; }
    else if (key == "--min-ms") o.minimum_ms = benchmark_arguments::Number<unsigned>(value, key, 5, 1000);
    else if (key == "--policy") {
      bool found = false;
      for (auto p : {Policy::Lazy, Policy::Auto, Policy::Simple, Policy::Static, Policy::Affinity})
        if (Name(p) == value) { o.policy = p; found = true; }
      if (!found) throw std::invalid_argument("unknown policy");
    } else if (key == "--placement") {
      bool found = false;
      for (auto p : {Placement::Local, Placement::All, Placement::Outsiders})
        if (Name(p) == value) { o.placement = p; found = true; }
      if (!found) throw std::invalid_argument("unknown placement");
    } else throw std::invalid_argument("unknown option");
  }
  if (!group_set) o.group = std::min(o.workers, 64u);
  if (!residents_set) o.residents = o.workers;
  Validate(o);
  return o;
}

unsigned Target(Placement placement, unsigned workers, unsigned group,
                unsigned publisher, size_t sequence) {
  const unsigned first = placement == Placement::Outsiders ? group : 0;
  const unsigned width = workers - first;
  const size_t start = static_cast<size_t>(publisher) * width / group;
  return first + static_cast<unsigned>((sequence % width + start + 1) % width);
}

Pool::Pool(const Options &o)
    : eigen::ThreadPool(o.workers, true, true, eigen::WorkerIdleMode::ResidentBusy),
      placement_(o.placement), group_(o.group), cursors_(std::make_unique<Cursor[]>(o.workers)) {
  SetResidentLimit(o.residents);
}
void Pool::Schedule(eigen::Task *task) {
  auto *context = rapid::current_resident_state;
  const size_t worker = CurrentThreadId();
#ifdef OOX_GROUP_DIAGNOSTICS
  if (context && &context->Pool() == this && worker < group_)
    ++cursors_[worker].counters.group_publications;
#endif
  if (placement_ == Placement::Local || !context || &context->Pool() != this || worker >= group_) {
    eigen::ThreadPool::Schedule(task);
    return;
  }
  // Each owning worker alone updates its padded cursor. No shared distributor
  // or global atomic counter is introduced. Ordinary descendant execution
  // outside the captured command keeps the usual local publication path.
  const unsigned target = Target(placement_, static_cast<unsigned>(NumThreads()),
      group_, static_cast<unsigned>(worker), cursors_[worker].next++);
#ifdef OOX_GROUP_DIAGNOSTICS
  ++cursors_[worker].counters.routed;
  cursors_[worker].counters.routed_outside += target >= group_;
#endif
  RunOnThread(task, target);
}
Pool::Counters Pool::Snapshot() const {
  Counters result;
#ifdef OOX_GROUP_DIAGNOSTICS
  for (size_t i = 0; i < NumThreads(); ++i) {
    result.group_publications += cursors_[i].counters.group_publications;
    result.routed += cursors_[i].counters.routed;
    result.routed_outside += cursors_[i].counters.routed_outside;
  }
#endif
  return result;
}

Runtime::Runtime(const Options &o)
    : options_((Validate(o), o)), pool_(o), domain_(pool_), group_{&domain_, {0, o.group}} {
  Prepare();
}
void Runtime::Prepare() {
  eigen_test_support::WaitForResidentWorkers(group_, std::chrono::seconds(5));
}
} // namespace rapid_group_experiment
