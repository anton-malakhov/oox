// SPDX-License-Identifier: Apache-2.0

#ifndef OOX_BACKENDS_SELECT_H
#define OOX_BACKENDS_SELECT_H

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
