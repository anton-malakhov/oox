// SPDX-License-Identifier: Apache-2.0
#include "papi_metrics.h"

#include <atomic>
#include <cstdlib>
#include <mutex>
#include <set>
#include <sstream>
#include <stdexcept>

#ifdef OOX_EVAL_HAVE_PAPI
#include <papi.h>
#endif

namespace scheduler_eval {

struct PapiSession {
  PapiResult result;
  std::mutex mutex;
  std::atomic<unsigned> active{0};
  std::atomic<bool> failed{false};
};

namespace {
std::shared_ptr<PapiSession> current;

void Fail(const std::shared_ptr<PapiSession> &session,
          const std::string &message) {
  std::lock_guard lock(session->mutex);
  if (session->result.error.empty())
    session->result.error = message;
  session->failed.store(true);
}

#ifdef OOX_EVAL_HAVE_PAPI
unsigned long ThreadId() {
  static std::atomic<unsigned long> next{1};
  static thread_local unsigned long id = next.fetch_add(1);
  return id;
}

struct ThreadCounters {
  int events = PAPI_NULL;
  unsigned depth{};
  bool registered{};
  std::vector<std::string> names;
  std::vector<long long> values;

  void Reset() {
    if (events != PAPI_NULL) {
      PAPI_cleanup_eventset(events);
      PAPI_destroy_eventset(&events);
    }
    names.clear();
  }
  ~ThreadCounters() {
    Reset();
    if (registered)
      PAPI_unregister_thread();
  }
};

thread_local ThreadCounters counters;

bool Check(int code, const char *operation,
           const std::shared_ptr<PapiSession> &session) {
  if (code == PAPI_OK)
    return true;
  Fail(session, std::string(operation) + ": " + PAPI_strerror(code));
  return false;
}
#endif
} // namespace

void BeginPapiMeasurement() {
  const char *requested = std::getenv("OOX_EVAL_PAPI_EVENTS");
  if (!requested || !*requested)
    return;
  auto session = std::make_shared<PapiSession>();
  std::istringstream input(requested);
  std::set<std::string> seen;
  std::string name;
  while (std::getline(input, name, ',')) {
    if (name.empty() || !seen.insert(name).second ||
        name.find_first_of(" \t\r\n") != std::string::npos)
      throw std::invalid_argument(
          "PAPI event names must be nonempty, distinct and comma-separated");
    session->result.events.push_back(name);
  }
  if (std::string(requested).back() == ',')
    throw std::invalid_argument("empty trailing PAPI event");
  session->result.values.resize(session->result.events.size());
#ifdef OOX_EVAL_HAVE_PAPI
  static std::once_flag initialized;
  std::call_once(initialized, [] {
    if (PAPI_library_init(PAPI_VER_CURRENT) != PAPI_VER_CURRENT ||
        PAPI_thread_init(ThreadId) != PAPI_OK)
      throw std::runtime_error("PAPI initialization failed");
  });
#else
  Fail(session, "PAPI requested but this build has no PAPI support");
#endif
  std::shared_ptr<PapiSession> empty;
  if (!std::atomic_compare_exchange_strong(&current, &empty, session))
    throw std::logic_error("overlapping PAPI measurement sessions");
}

bool PapiMeasurementActive() { return bool(std::atomic_load(&current)); }

PapiResult EndPapiMeasurement() {
  auto session = std::atomic_exchange(&current, std::shared_ptr<PapiSession>{});
  if (!session)
    return {};
  std::lock_guard lock(session->mutex);
  if (session->active.load() && session->result.error.empty())
    session->result.error =
        "PAPI measurement ended with active callback regions";
  return session->result;
}

PapiRegion::PapiRegion() : session_(std::atomic_load(&current)) {
  if (!session_)
    return;
  if (session_->failed.load()) {
    session_.reset();
    return;
  }
#ifdef OOX_EVAL_HAVE_PAPI
  outer_ = counters.depth++ == 0;
  if (!outer_)
    return;
  session_->active.fetch_add(1);
  if (!counters.registered) {
    if (!Check(PAPI_register_thread(), "PAPI_register_thread", session_))
      return;
    counters.registered = true;
  }
  if (counters.names != session_->result.events) {
    counters.Reset();
    if (!Check(PAPI_create_eventset(&counters.events), "PAPI_create_eventset",
               session_))
      return;
    for (const auto &event : session_->result.events) {
      if (!Check(PAPI_add_named_event(counters.events, event.c_str()),
                 event.c_str(), session_)) {
        counters.Reset();
        return;
      }
    }
    counters.names = session_->result.events;
    counters.values.resize(counters.names.size());
  }
  started_ = Check(PAPI_start(counters.events), "PAPI_start", session_);
#endif
}

PapiRegion::~PapiRegion() {
#ifdef OOX_EVAL_HAVE_PAPI
  if (!session_)
    return;
  --counters.depth;
  if (!outer_)
    return;
  if (started_) {
    if (Check(PAPI_stop(counters.events, counters.values.data()), "PAPI_stop",
              session_)) {
      std::lock_guard lock(session_->mutex);
      for (std::size_t i = 0; i < counters.values.size(); ++i)
        session_->result.values[i] += counters.values[i];
      ++session_->result.regions;
    } else {
      counters.Reset();
    }
  }
  session_->active.fetch_sub(1);
#endif
}

} // namespace scheduler_eval
