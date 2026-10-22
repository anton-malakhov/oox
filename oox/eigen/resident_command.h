#pragma once
// This file is part of Eigen, a lightweight C++ template library
// for linear algebra.
//
// Copyright (C) 2016 Dmitry Vyukov <dvyukov@google.com>
//
// This Source Code Form is subject to the terms of the Mozilla
// Public License v. 2.0. If a copy of the MPL was not distributed
// with this file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// PBBS mailbox extensions were imported from EgorkaZ/pbbsbench's
// eigen-mailbox branch. See README.md in this directory for provenance and
// the OOX-specific adaptations retained here.

#include <atomic>
#include <cstddef>
#include <cstdint>

namespace oox::detail::eigen_pool {

class ResidentTask {
public:
  virtual void Run(size_t slot) noexcept = 0;
  virtual ~ResidentTask() = default;
};

namespace internal {

struct alignas(128) ResidentBroadcast {
  std::atomic<uint64_t> generation{0};
  alignas(128) std::atomic_flag busy = ATOMIC_FLAG_INIT;
  std::atomic<uint64_t> mask{0};
  ResidentTask *task = nullptr;
  std::atomic<size_t> *completion = nullptr;
  uint64_t epoch = 0;
  bool packed_completion = false;
  size_t first_slot = 1;
};

} // namespace internal
} // namespace oox::detail::eigen_pool
