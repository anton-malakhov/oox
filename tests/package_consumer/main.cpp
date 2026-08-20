// SPDX-License-Identifier: Apache-2.0

#include <oox/oox.h>

int main() {
  auto value = oox::run([] { return 42; });
  return oox::wait_and_get(value) == 42 ? 0 : 1;
}
