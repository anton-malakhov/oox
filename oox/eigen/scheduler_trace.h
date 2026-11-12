// SPDX-License-Identifier: Apache-2.0
#pragma once

// Opt-in diagnostic ABI. Define consistently in every TU of a traced program
// and link benchmarks/scheduler_eval/tracing/recorder.cpp. No arguments are
// evaluated and no task fields are added in normal builds.
#ifdef OOX_SCHEDULER_TRACE
#include <cstdint>
namespace oox::scheduler_trace {
using Id = std::uint64_t;
Id NextId() noexcept;
Id Current() noexcept;
enum class Counter : unsigned {
  RangeLockAcquired, RangeTagContended, RangeRejectedPublishing,
  RangeRejectedCommand, RangeRejectedEmpty, RangeRejectedGrain,
  OwnerWaitSpins, Count
};
void Count(Counter counter, std::uint64_t amount = 1) noexcept;
void Worker(int worker) noexcept;
bool Emit(const char *kind, Id id, Id a = 0, Id b = 0, Id parent = 0,
          char phase = 'i') noexcept;
class Scope {
public:
  Scope(const char *kind, Id id, Id a = 0, Id b = 0) noexcept;
  ~Scope();
  Scope(const Scope &) = delete;
  Scope &operator=(const Scope &) = delete;
private:
  const char *kind_;
  Id id_, previous_;
  bool recorded_;
};
}
#define OOX_SCHED_EVENT(...) ::oox::scheduler_trace::Emit(__VA_ARGS__)
#define OOX_SCHED_WORKER(worker) ::oox::scheduler_trace::Worker(static_cast<int>(worker))
#define OOX_SCHED_SCOPE(...) ::oox::scheduler_trace::Scope oox_trace_scope(__VA_ARGS__)
#define OOX_SCHED_COUNT(counter, ...) ::oox::scheduler_trace::Count(::oox::scheduler_trace::Counter::counter __VA_OPT__(,) __VA_ARGS__)
#else
#define OOX_SCHED_EVENT(...) ((void)0)
#define OOX_SCHED_WORKER(...) ((void)0)
#define OOX_SCHED_SCOPE(...) ((void)0)
#define OOX_SCHED_COUNT(...) ((void)0)
#endif
