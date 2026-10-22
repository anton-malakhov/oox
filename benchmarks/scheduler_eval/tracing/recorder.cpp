// SPDX-License-Identifier: Apache-2.0
#include "recorder.h"
#include <atomic>
#include <chrono>
#include <iomanip>
#include <memory>
#include <ostream>
#include <stdexcept>
#include <thread>

namespace oox::scheduler_trace {
namespace {
struct Event {
  std::uint64_t ns;
  Id id, parent, a, b;
  const char *kind;
  int worker;
  char phase;
};
struct alignas(128) Buffer {
  std::unique_ptr<Event[]> events;
  std::atomic<bool> writing{false};
  std::atomic<std::size_t> open_scopes{0};
  std::size_t used = 0, dropped = 0;
  std::size_t recorded_begins = 0, recorded_ends = 0, rejected_scopes = 0;
  std::uint64_t counters[static_cast<unsigned>(Counter::Count)]{};
};
struct Recorder {
  std::unique_ptr<Buffer[]> buffers;
  std::size_t lanes = 0, capacity = 0;
  std::atomic<bool> enabled{false};
  std::atomic<bool> accepting_scopes{false};
  std::atomic<std::size_t> next_lane{0}, overflow{0};
  std::chrono::steady_clock::time_point origin;
};
// Stable through scheduler/static destructors. Only a tiny control object is
// intentionally process-lifetime; buffers are released after Dump joins Emit.
Recorder &Get() { static auto *value = new Recorder; return *value; }
std::atomic<Id> ids{1};
thread_local Id current = 0;
thread_local int worker = -1;
thread_local std::size_t lane = std::size_t(-1);
#ifdef OOX_TRACE_RECORDER_TESTING
void (*closing_hook)() = nullptr;
#endif
bool Record(Recorder &r, Buffer &buffer, const char *kind, Id id, Id a, Id b,
            Id parent, char phase) noexcept {
  if (buffer.used == r.capacity) { ++buffer.dropped; return false; }
  const auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
      std::chrono::steady_clock::now() - r.origin).count();
  buffer.events[buffer.used++] = {static_cast<std::uint64_t>(ns), id, parent, a, b, kind, worker, phase};
  buffer.recorded_begins += phase == 'B';
  buffer.recorded_ends += phase == 'E';
  return true;
}
bool BeginScope(const char *kind, Id id, Id a, Id b, Id parent) noexcept {
  auto &r = Get();
  if (!r.enabled.load(std::memory_order_seq_cst)) return false;
  if (lane == std::size_t(-1)) lane = r.next_lane.fetch_add(1, std::memory_order_relaxed);
  if (lane >= r.lanes) { r.overflow.fetch_add(1, std::memory_order_relaxed); return false; }
  auto &buffer = r.buffers[lane];
  buffer.writing.store(true, std::memory_order_seq_cst);
  bool recorded = false;
  if (r.enabled.load(std::memory_order_seq_cst)) {
    if (!r.accepting_scopes.load(std::memory_order_seq_cst)) {
      ++buffer.rejected_scopes;
    } else {
      // Reserve before the admission recheck. A successful recheck precedes
      // Dump's SC close, so its drain sees this reservation until our end.
      buffer.open_scopes.fetch_add(1, std::memory_order_seq_cst);
      if (r.accepting_scopes.load(std::memory_order_seq_cst))
        recorded = Record(r, buffer, kind, id, a, b, parent, 'B');
      else
        ++buffer.rejected_scopes;
      if (!recorded) buffer.open_scopes.fetch_sub(1, std::memory_order_seq_cst);
    }
  }
  buffer.writing.store(false, std::memory_order_seq_cst);
  return recorded;
}
void String(std::ostream &out, std::string_view value) {
  out << '"';
  for (unsigned char c : value) {
    if (c == '"' || c == '\\') out << '\\' << c;
    else if (c < 32) out << "\\u00" << "0123456789abcdef"[c >> 4] << "0123456789abcdef"[c & 15];
    else out << c;
  }
  out << '"';
}
}
Id NextId() noexcept { return ids.fetch_add(1, std::memory_order_relaxed); }
Id Current() noexcept { return current; }
void Worker(int value) noexcept { worker = value; }
#ifdef OOX_TRACE_RECORDER_TESTING
void SetClosingHook(void (*hook)()) noexcept { closing_hook = hook; }
#endif
void Count(Counter counter, std::uint64_t amount) noexcept {
  if (!amount) return;
  auto &r = Get();
  if (!r.enabled.load(std::memory_order_seq_cst)) return;
  if (lane == std::size_t(-1)) lane = r.next_lane.fetch_add(1, std::memory_order_relaxed);
  if (lane >= r.lanes) return;
  auto &buffer = r.buffers[lane];
  buffer.writing.store(true, std::memory_order_seq_cst);
  if (r.enabled.load(std::memory_order_seq_cst))
    buffer.counters[static_cast<unsigned>(counter)] += amount;
  buffer.writing.store(false, std::memory_order_seq_cst);
}
bool Emit(const char *kind, Id id, Id a, Id b, Id parent, char phase) noexcept {
  auto &r = Get();
  if (!r.enabled.load(std::memory_order_seq_cst)) return false;
  if (lane == std::size_t(-1)) lane = r.next_lane.fetch_add(1, std::memory_order_relaxed);
  if (lane >= r.lanes) { r.overflow.fetch_add(1, std::memory_order_relaxed); return false; }
  auto &buffer = r.buffers[lane];
  buffer.writing.store(true, std::memory_order_seq_cst);
  bool recorded = false;
  // SC ensures a writer either precedes Stop's join or sees disabled here.
  if (r.enabled.load(std::memory_order_seq_cst))
    recorded = Record(r, buffer, kind, id, a, b, parent, phase);
  buffer.writing.store(false, std::memory_order_seq_cst);
  return recorded;
}
Scope::Scope(const char *kind, Id id, Id a, Id b) noexcept
    : kind_(kind), id_(id), previous_(current), recorded_(BeginScope(kind_, id_, a, b, previous_)) {
  current = id_;
}
Scope::~Scope() {
  current = previous_;
  if (recorded_) {
    Emit(kind_, id_, 0, 0, previous_, 'E');
    Get().buffers[lane].open_scopes.fetch_sub(1, std::memory_order_seq_cst);
  }
}
void Start(std::size_t lanes, std::size_t capacity) {
  auto &r = Get();
  if (r.lanes) throw std::logic_error("only one trace recording per process");
  if (!lanes || lanes > 128 || !capacity || capacity > 1048576 || lanes * capacity > 4194304)
    throw std::invalid_argument("trace buffer limits exceeded (max 4M events)");
  auto buffers = std::make_unique<Buffer[]>(lanes);
  for (std::size_t i = 0; i < lanes; ++i) buffers[i].events = std::make_unique<Event[]>(capacity);
  r.lanes = lanes;
  r.capacity = capacity;
  r.buffers = std::move(buffers);
  r.origin = std::chrono::steady_clock::now();
  r.accepting_scopes.store(true, std::memory_order_seq_cst);
  r.enabled.store(true, std::memory_order_seq_cst);
}
void Dump(std::ostream &out, std::string_view mode, std::string_view workload,
          std::size_t workers, std::size_t tasks, std::size_t iterations,
          std::size_t work, std::size_t grain, bool oracle_ok) {
  auto &r = Get();
  if (!r.accepting_scopes.exchange(false, std::memory_order_seq_cst))
    throw std::logic_error("no active trace recording");
  const auto drain_begin = std::chrono::steady_clock::now();
  const auto deadline = drain_begin + std::chrono::milliseconds(250);
#ifdef OOX_TRACE_RECORDER_TESTING
  if (closing_hook) closing_hook();
#endif
  bool drain_timed_out = false;
  for (;;) {
    std::size_t pending = 0;
    for (std::size_t i = 0; i < r.lanes; ++i)
      pending += r.buffers[i].open_scopes.load(std::memory_order_seq_cst);
    if (!pending) break;
    if (std::chrono::steady_clock::now() >= deadline) { drain_timed_out = true; break; }
    std::this_thread::yield();
  }
  const auto drain_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
      std::chrono::steady_clock::now() - drain_begin).count();
  r.enabled.store(false, std::memory_order_seq_cst);
  // Keep Buffer objects alive: a late emitter may set writing, see disabled,
  // and return. Its Event storage is no longer accessed after this join.
  for (std::size_t i = 0; i < r.lanes; ++i)
    while (r.buffers[i].writing.load(std::memory_order_seq_cst)) std::this_thread::yield();
  std::size_t pending = 0, truncated = 0, rejected = 0;
  for (std::size_t i = 0; i < r.lanes; ++i) {
    const auto &buffer = r.buffers[i];
    pending += buffer.open_scopes.load(std::memory_order_seq_cst);
    truncated += buffer.recorded_begins - buffer.recorded_ends;
    rejected += buffer.rejected_scopes;
  }
  out << "{\"schema\":\"oox-scheduler-trace-v1\",\"mode\":";
  String(out, mode);
  out << ",\"workload\":"; String(out, workload);
  out << ",\"workers\":" << workers << ",\"tasks\":" << tasks << ",\"iterations\":" << iterations
      << ",\"work\":" << work << ",\"grain\":" << grain << ",\"oracle_ok\":" << (oracle_ok ? "true" : "false")
      << ",\"instrumented\":true,\"events\":[";
  bool comma = false;
  std::size_t dropped = r.overflow.load();
  for (std::size_t i = 0; i < r.lanes; ++i) {
    auto &buffer = r.buffers[i];
    dropped += buffer.dropped;
    for (std::size_t j = 0; j < buffer.used; ++j) {
      const auto &e = buffer.events[j];
      if (comma) out << ',';
      comma = true;
      out << "{\"ns\":" << e.ns << ",\"tid\":" << i << ",\"worker\":" << e.worker << ",\"kind\":";
      String(out, e.kind);
      out << ",\"ph\":\"" << e.phase << "\",\"id\":" << e.id << ",\"parent\":" << e.parent
          << ",\"a\":" << e.a << ",\"b\":" << e.b << '}';
    }
    buffer.events.reset();
  }
  out << "],\"dropped_events\":" << dropped
      << ",\"scope_drain_timeout_ms\":250,\"scope_drain_ns\":" << drain_ns
      << ",\"scope_drain_timed_out\":" << (drain_timed_out ? "true" : "false")
      << ",\"pending_scopes\":" << pending << ",\"truncated_scopes\":" << truncated
      << ",\"closing_rejected_scopes\":" << rejected << ",\"thread_counters\":[";
  constexpr const char *names[] = {"range_lock_acquired", "range_tag_contended",
      "range_rejected_publishing", "range_rejected_command", "range_rejected_empty",
      "range_rejected_grain", "owner_wait_spins"};
  static_assert(std::size(names) == static_cast<unsigned>(Counter::Count));
  for (std::size_t i = 0; i < r.lanes; ++i) {
    if (i) out << ',';
    out << "{\"tid\":" << i;
    for (unsigned k = 0; k < static_cast<unsigned>(Counter::Count); ++k)
      out << ",\"" << names[k] << "\":" << r.buffers[i].counters[k];
    out << '}';
  }
  out << "]}\n";
}
}
