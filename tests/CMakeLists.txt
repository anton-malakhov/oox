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
    if (OOX_EXCEPTIONS_ENABLED)
      target_compile_definitions(${unittest}_tbb PRIVATE TBB_USE_EXCEPTIONS=1)
    endif ()
    target_link_libraries(${unittest}_tbb
            PRIVATE
            ${PROJECT_NAME}
            ${OOX_GTEST_LIB}
            TBB::tbb
    )
    gtest_discover_tests(${unittest}_tbb)
  endif ()

  if (OOX_ENABLE_EIGEN)
    add_executable(${unittest}_eigen ${unittest}.cpp)
    target_link_libraries(${unittest}_eigen PRIVATE
            ${PROJECT_NAME}::eigen
            ${OOX_GTEST_LIB}
    )
    gtest_discover_tests(${unittest}_eigen)
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

if (OOX_ENABLE_EIGEN)
  add_executable(eigen_pool_test eigen_pool_test.cpp)
  target_link_libraries(eigen_pool_test PRIVATE
          ${PROJECT_NAME}::eigen
          ${OOX_GTEST_LIB})
  gtest_discover_tests(eigen_pool_test
          DISCOVERY_MODE PRE_TEST
          DISCOVERY_TIMEOUT 30)

  add_executable(eigen_pool_instantiation eigen_pool_instantiation.cpp)
  target_link_libraries(eigen_pool_instantiation PRIVATE ${PROJECT_NAME}::eigen)
  add_test(NAME EigenPoolExplicitInstantiation COMMAND eigen_pool_instantiation)

  add_executable(eigen_rapid_start_test eigen_rapid_start_test.cpp)
  target_link_libraries(eigen_rapid_start_test PRIVATE
          ${PROJECT_NAME}::eigen
          ${OOX_GTEST_LIB})
  gtest_discover_tests(eigen_rapid_start_test
          DISCOVERY_MODE PRE_TEST
          DISCOVERY_TIMEOUT 30)

  # Standalone (no gtest) policy-matrix checks. These run every GrainLaw x
  # VictimPolicy combination and were validated under TSan/ASan/UBSan on Linux.
  add_executable(policy_matrix_check policy_matrix_check.cpp)
  target_link_libraries(policy_matrix_check PRIVATE ${PROJECT_NAME}::eigen)
  add_test(NAME EigenRapidPolicyMatrix COMMAND policy_matrix_check)
  set_tests_properties(EigenRapidPolicyMatrix PROPERTIES TIMEOUT 600)

  add_executable(wake_check wake_check.cpp)
  target_link_libraries(wake_check PRIVATE ${PROJECT_NAME}::eigen)
  add_test(NAME EigenBatchedWakeCount COMMAND wake_check)

  find_package(Eigen3 3.4 QUIET NO_MODULE)
  if (TARGET Eigen3::Eigen)
    set(OOX_UPSTREAM_EIGEN_TARGET Eigen3::Eigen)
  else ()
    FetchContent_Declare(
            oox_upstream_eigen
            GIT_REPOSITORY https://gitlab.com/libeigen/eigen.git
            GIT_TAG 3.4.0
            GIT_SHALLOW TRUE)
    FetchContent_GetProperties(oox_upstream_eigen)
    if (NOT oox_upstream_eigen_POPULATED)
      FetchContent_Populate(oox_upstream_eigen)
    endif ()
    add_library(oox_upstream_eigen_headers INTERFACE)
    target_include_directories(oox_upstream_eigen_headers INTERFACE
            ${oox_upstream_eigen_SOURCE_DIR})
    set(OOX_UPSTREAM_EIGEN_TARGET oox_upstream_eigen_headers)
  endif ()

  foreach (include_order IN ITEMS oox_first upstream_first)
    add_executable(eigen_include_${include_order}
            eigen_include_${include_order}.cpp)
    target_link_libraries(eigen_include_${include_order} PRIVATE
            ${PROJECT_NAME}::eigen
            ${OOX_UPSTREAM_EIGEN_TARGET})
    add_test(NAME EigenIncludeOrder_${include_order}
             COMMAND eigen_include_${include_order})
  endforeach ()

endif ()

if (OOX_EXCEPTIONS_ENABLED)
  add_test(NAME ExceptionPolicyCompile
           COMMAND ${CMAKE_COMMAND}
                   -D OOX_SOURCE_DIR=${PROJECT_SOURCE_DIR}
                   -D OOX_BINARY_DIR=${PROJECT_BINARY_DIR}
                   -P ${CMAKE_CURRENT_LIST_DIR}/exception_policy_compile.cmake)
endif ()

if (OOX_BUILD_TWIST_TESTS)
  add_subdirectory(twist)
endif ()
