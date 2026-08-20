// This file is part of Eigen, a lightweight C++ template library
// for linear algebra.
//
// Copyright (C) 2008-2015 Gael Guennebaud <gael.guennebaud@inria.fr>
// Copyright (C) 2008-2009 Benoit Jacob <jacob.benoit.1@gmail.com>
// Copyright (C) 2009 Kenneth Riddile <kfriddile@yahoo.com>
// Copyright (C) 2010 Hauke Heibel <hauke.heibel@gmail.com>
// Copyright (C) 2010 Thomas Capricelli <orzel@freehackers.org>
// Copyright (C) 2013 Pavel Holoborodko <pavel@holoborodko.com>
//
// This Source Code Form is subject to the terms of the Mozilla
// Public License v. 2.0. If a copy of the MPL was not distributed
// with this file, You can obtain one at http://mozilla.org/MPL/2.0/.

#ifndef OOX_EIGEN_MEMORY_H
#define OOX_EIGEN_MEMORY_H

#include <cassert>
#include <cstdlib>
#include <cstdint>

namespace oox::detail::eigen_pool::internal {
inline void *handmade_aligned_malloc(std::size_t size, std::size_t alignment) {
  assert(alignment >= sizeof(void *) && alignment <= 128 &&
         (alignment & (alignment - 1)) == 0 &&
         "Alignment must be at least sizeof(void*), less than or equal "
         "to 128, and a power of 2");
  void *original = std::malloc(size + alignment);
  if (original == 0)
    return 0;
  uint8_t offset = static_cast<uint8_t>(
      alignment - (reinterpret_cast<std::size_t>(original) & (alignment - 1)));
  void *aligned =
      static_cast<void *>(static_cast<uint8_t *>(original) + offset);
  *(static_cast<uint8_t *>(aligned) - 1) = offset;
  return aligned;
}

inline void handmade_aligned_free(void *ptr) {
  if (ptr) {
    uint8_t offset = static_cast<uint8_t>(*(static_cast<uint8_t *>(ptr) - 1));
    void *original = static_cast<void *>(static_cast<uint8_t *>(ptr) - offset);
    std::free(original);
  }
}
} // namespace oox::detail::eigen_pool::internal

#endif // OOX_EIGEN_MEMORY_H
