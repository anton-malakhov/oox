// SPDX-License-Identifier: Apache-2.0
#pragma once

#include <memory>
#include <string>
#include <vector>

namespace scheduler_eval {

struct PapiSession;
struct PapiResult {
  std::vector<std::string> events;
  std::vector<long long> values;
  std::string error;
  unsigned long long regions{};
};

void BeginPapiMeasurement();
bool PapiMeasurementActive();
PapiResult EndPapiMeasurement();

class PapiRegion {
public:
  PapiRegion();
  ~PapiRegion();
  PapiRegion(const PapiRegion &) = delete;
  PapiRegion &operator=(const PapiRegion &) = delete;

private:
  std::shared_ptr<PapiSession> session_;
  bool outer_{};
  bool started_{};
};

} // namespace scheduler_eval
