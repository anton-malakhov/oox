// SPDX-License-Identifier: Apache-2.0

#ifndef OOX_BACKENDS_SELECT_H
#define OOX_BACKENDS_SELECT_H

// OOX_SERIAL_DEBUG intentionally overrides every asynchronous backend. Outside
// serial-debug builds, enabling more than one backend is an error: selecting by
// incidental preprocessor order would make consumer behavior non-portable.
#if !OOX_SERIAL_DEBUG &&                                                   \
    ((defined(HAVE_OMP) && HAVE_OMP) +                                    \
     (defined(HAVE_TBB) && HAVE_TBB) +                                    \
     (defined(HAVE_TF) && HAVE_TF) +                                      \
     (defined(HAVE_TWIST) && HAVE_TWIST) +                                \
     (defined(HAVE_FOLLY) && HAVE_FOLLY) +                                \
     (defined(HAVE_EIGEN) && HAVE_EIGEN) > 1)
#error "Enable exactly one OOX asynchronous backend"
#endif

#if OOX_SERIAL_DEBUG
#include "serial/backend.h"
#elif HAVE_OMP
#include "openmp/backend.h"
#elif HAVE_TBB
#include "tbb/backend.h"
#elif HAVE_TF
#include "taskflow/backend.h"
#elif HAVE_TWIST
#include "twist/backend.h"
#elif HAVE_FOLLY
#include "folly/backend.h"
#elif HAVE_EIGEN
#include "eigen/backend.h"
#else
#include "std/backend.h"
#endif

#endif // OOX_BACKENDS_SELECT_H
