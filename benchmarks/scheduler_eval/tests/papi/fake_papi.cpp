// SPDX-License-Identifier: Apache-2.0
#include "papi.h"
#include <map>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

namespace {
struct EventSet {
  std::thread::id owner;
  std::vector<std::string> names;
  bool running{};
};
std::map<int, EventSet> sets;
std::mutex mutex;
int next_set{};
thread_local bool registered{};
} // namespace

extern "C" {
int PAPI_library_init(int version) { return version; }
int PAPI_thread_init(unsigned long (*id)()) { return id ? 0 : -2; }
int PAPI_register_thread() {
  registered = true;
  return 0;
}
int PAPI_unregister_thread() {
  registered = false;
  return 0;
}
int PAPI_create_eventset(int *id) {
  std::lock_guard lock(mutex);
  if (!registered)
    return -2;
  *id = ++next_set;
  sets[*id] = {std::this_thread::get_id(), {}, false};
  return 0;
}
int PAPI_cleanup_eventset(int id) {
  std::lock_guard lock(mutex);
  auto &set = sets.at(id);
  if (set.running || set.owner != std::this_thread::get_id())
    return -2;
  set.names.clear();
  return 0;
}
int PAPI_destroy_eventset(int *id) {
  std::lock_guard lock(mutex);
  const auto &set = sets.at(*id);
  if (set.running || set.owner != std::this_thread::get_id())
    return -2;
  sets.erase(*id);
  *id = PAPI_NULL;
  return 0;
}
int PAPI_add_named_event(int id, const char *name) {
  std::lock_guard lock(mutex);
  if (std::string(name) == "BAD")
    return -2;
  sets.at(id).names.emplace_back(name);
  return 0;
}
int PAPI_start(int id) {
  std::lock_guard lock(mutex);
  auto &set = sets.at(id);
  if (set.running || set.owner != std::this_thread::get_id() ||
      set.names[0] == "FAIL_START")
    return -2;
  set.running = true;
  return 0;
}
int PAPI_stop(int id, long long *values) {
  std::lock_guard lock(mutex);
  auto &set = sets.at(id);
  if (!set.running || set.owner != std::this_thread::get_id())
    return -2;
  set.running = false;
  if (set.names[0] == "FAIL_STOP")
    return -2;
  for (std::size_t i = 0; i < set.names.size(); ++i)
    values[i] = 7 * (i + 1);
  return 0;
}
char *PAPI_strerror(int) {
  static char error[] = "fake PAPI failure";
  return error;
}
}
