// SPDX-License-Identifier: Apache-2.0
#include "common.h"

int main(int argc, char **argv) {
  std::string text;
  for (int c = 0; c < 32; ++c)
    text.push_back(static_cast<char>(c));
  text += "\"\\/UTF-8: \xc3\xa9";
  std::cout << "{\"threads\":" << GetNumThreads()
            << ",\"value\":" << scheduler_eval::Argument(argc, argv, "--value", 7)
            << ",\"text\":\"" << scheduler_eval::JsonEscape(text) << "\"}\n";
}
