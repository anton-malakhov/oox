# Copyright (C) 2021 Intel Corporation
#
# SPDX-License-Identifier: Apache-2.0

find_package(GTest REQUIRED)
include(GoogleTest)

# Pick the right GTest library target depending on CMake version
if (TARGET GTest::gtest)
  set(OOX_GTEST_LIB GTest::gtest)
elseif (TARGET GTest::GTest)  # older CMake FindGTest
  set(OOX_GTEST_LIB GTest::GTest)
else ()
  message(FATAL_ERROR "GTest found, but no suitable GTest library target (GTest::gtest / GTest::GTest)")
endif ()

list(APPEND OOX_UNITTESTS
        test_oox
)

foreach (unittest IN LISTS OOX_UNITTESTS)

  # Default implementation
  add_executable(${unittest} ${unittest}.cpp)
  target_link_libraries(${unittest}
          PRIVATE
          ${PROJECT_NAME}
          ${OOX_GTEST_LIB}
  )
  gtest_discover_tests(${unittest})

  # Serial debugging implementation
  add_executable(${unittest}_serial ${unittest}.cpp)
  target_compile_definitions(${unittest}_serial PRIVATE OOX_SERIAL_DEBUG)
  target_link_libraries(${unittest}_serial
          PRIVATE
          ${PROJECT_NAME}
          ${OOX_GTEST_LIB}
  )
  gtest_discover_tests(${unittest}_serial)

  if (TBB_FOUND)
    # build test in TBB mode as well
    add_executable(${unittest}_tbb ${unittest}.cpp)
    target_compile_definitions(${unittest}_tbb PRIVATE HAVE_TBB)
    target_link_libraries(${unittest}_tbb
            PRIVATE
            ${PROJECT_NAME}
            ${OOX_GTEST_LIB}
            TBB::tbb
    )
    gtest_discover_tests(${unittest}_tbb)
  endif ()

  if (Taskflow_FOUND)
    # build test in TF mode as well
    add_executable(${unittest}_tf ${unittest}.cpp)
    target_compile_definitions(${unittest}_tf PRIVATE HAVE_TF)
    target_link_libraries(${unittest}_tf
            PRIVATE
            ${PROJECT_NAME}
            ${OOX_GTEST_LIB}
            Taskflow::Taskflow
    )
    gtest_discover_tests(${unittest}_tf)
  endif ()

endforeach ()
