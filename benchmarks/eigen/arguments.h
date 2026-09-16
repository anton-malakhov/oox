// SPDX-License-Identifier: Apache-2.0
#pragma once

#include <charconv>
#include <cstdio>
#include <cstdlib>
#include <limits>
#include <string_view>

namespace benchmark_arguments {
template <typename T>
T Number(std::string_view text, std::string_view name, T minimum = 0,
         T maximum = std::numeric_limits<T>::max()) {
  T value{};
  bool valid = !text.empty();
  if (valid) {
    const auto result = std::from_chars(text.data(), text.data() + text.size(), value);
    valid = result.ec == std::errc{} && result.ptr == text.data() + text.size() &&
            value >= minimum && value <= maximum;
  }
  if (!valid) {
    std::fprintf(stderr, "%.*s must be a decimal integer in the supported range\n",
                 static_cast<int>(name.size()), name.data());
    std::exit(2);
  }
  return value;
}
} // namespace benchmark_arguments
