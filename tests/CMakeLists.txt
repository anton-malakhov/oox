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
        test_shared_var
)

foreach (unittest IN LISTS OOX_UNITTESTS)

  # Default implementation
  add_executable(${unittest} ${unittest}.cpp)
  target_link_libraries(${unittest}
          PRIVATE
          ${PROJECT_NAME}
          ${OOX_GTEST_LIB}
  )
  if (unittest STREQUAL "test_shared_var")
    target_compile_definitions(${unittest}
            PRIVATE OOX_TEST_INJECT_TASK_SPAWN_FAILURE=1)
  endif ()
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
    if (unittest STREQUAL "test_shared_var")
      target_compile_definitions(${unittest}_tbb
              PRIVATE OOX_TEST_INJECT_TASK_SPAWN_FAILURE=1)
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
    if (unittest STREQUAL "test_shared_var")
      target_compile_definitions(${unittest}_tf
              PRIVATE OOX_TEST_INJECT_TASK_SPAWN_FAILURE=1)
    endif ()
    target_link_libraries(${unittest}_tf
            PRIVATE
            ${PROJECT_NAME}
            ${OOX_GTEST_LIB}
            Taskflow::Taskflow
    )
    gtest_discover_tests(${unittest}_tf)
  endif ()

endforeach ()

add_test(NAME NonThrowConsumeCompile
         COMMAND ${CMAKE_COMMAND}
                 -D OOX_SOURCE_DIR=${PROJECT_SOURCE_DIR}
                 -D OOX_BINARY_DIR=${PROJECT_BINARY_DIR}
                 "-D OOX_GENERATOR=${CMAKE_GENERATOR}"
                 "-D OOX_GENERATOR_PLATFORM=${CMAKE_GENERATOR_PLATFORM}"
                 "-D OOX_GENERATOR_TOOLSET=${CMAKE_GENERATOR_TOOLSET}"
                 "-D OOX_GENERATOR_INSTANCE=${CMAKE_GENERATOR_INSTANCE}"
                 "-D OOX_MAKE_PROGRAM=${CMAKE_MAKE_PROGRAM}"
                 "-D OOX_CXX_COMPILER=${CMAKE_CXX_COMPILER}"
                 "-D OOX_CXX_COMPILER_TARGET=${CMAKE_CXX_COMPILER_TARGET}"
                 "-D OOX_TOOLCHAIN_FILE=${CMAKE_TOOLCHAIN_FILE}"
                 "-D OOX_SYSROOT=${CMAKE_SYSROOT}"
                 "-D OOX_OSX_SYSROOT=${CMAKE_OSX_SYSROOT}"
                 "-D OOX_OSX_ARCHITECTURES=${CMAKE_OSX_ARCHITECTURES}"
                 "-D OOX_OSX_DEPLOYMENT_TARGET=${CMAKE_OSX_DEPLOYMENT_TARGET}"
                 "-D OOX_BUILD_TYPE=${CMAKE_BUILD_TYPE}"
                 -P ${CMAKE_CURRENT_LIST_DIR}/nonthrow_consume_compile.cmake)
if (OOX_ENABLE_EIGEN)
  add_executable(eigen_pool_test eigen_pool_test.cpp)
  target_compile_definitions(eigen_pool_test PRIVATE OOX_EIGEN_ENABLE_STATS=1)
  target_link_libraries(eigen_pool_test PRIVATE
          ${PROJECT_NAME}::eigen
          ${OOX_GTEST_LIB})
  gtest_discover_tests(eigen_pool_test
          DISCOVERY_MODE PRE_TEST
          DISCOVERY_TIMEOUT 30)

  add_executable(eigen_small_object_pool_test eigen_small_object_pool_test.cpp)
  target_link_libraries(eigen_small_object_pool_test PRIVATE
          ${PROJECT_NAME}::eigen ${OOX_GTEST_LIB})
  target_compile_definitions(eigen_small_object_pool_test PRIVATE
          OOX_EIGEN_SMALL_OBJECT_POOL_TESTING=1)
  gtest_discover_tests(eigen_small_object_pool_test
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

  add_executable(eigen_rapid_mailbox_test eigen_rapid_mailbox_test.cpp)
  target_link_libraries(eigen_rapid_mailbox_test PRIVATE
          ${PROJECT_NAME}::eigen ${OOX_GTEST_LIB})
  target_compile_definitions(eigen_rapid_mailbox_test PRIVATE
          OOX_EIGEN_ENABLE_STATS=1)
  gtest_discover_tests(eigen_rapid_mailbox_test
          DISCOVERY_MODE PRE_TEST DISCOVERY_TIMEOUT 30
          PROPERTIES TIMEOUT 90)

  add_executable(eigen_partitioner_range_test eigen_partitioner_range_test.cpp)
  target_link_libraries(eigen_partitioner_range_test PRIVATE ${PROJECT_NAME}::eigen)

  add_executable(eigen_partitioner_allocation_test eigen_partitioner_allocation_test.cpp)
  target_link_libraries(eigen_partitioner_allocation_test PRIVATE ${PROJECT_NAME}::eigen)
  # Inject failures at every allocation request, including warm-cache calls.
  target_compile_definitions(eigen_partitioner_allocation_test PRIVATE
          OOX_EIGEN_SMALL_OBJECT_POOL=0)
  add_executable(eigen_partitioner_lifetime_test eigen_partitioner_lifetime_test.cpp)
  target_link_libraries(eigen_partitioner_lifetime_test PRIVATE ${PROJECT_NAME}::eigen)
  # Observe actual Region reclamation rather than retention in the object cache.
  target_compile_definitions(eigen_partitioner_lifetime_test PRIVATE
          OOX_EIGEN_SMALL_OBJECT_POOL=0)
  add_test(NAME EigenPartitionerLifetimeOracle COMMAND eigen_partitioner_lifetime_test)

  add_executable(eigen_partitioner_completion_test eigen_partitioner_completion_test.cpp)
  target_link_libraries(eigen_partitioner_completion_test PRIVATE ${PROJECT_NAME}::eigen)
  target_compile_definitions(eigen_partitioner_completion_test PRIVATE
          OOX_EIGEN_THREAD_POOL_TESTING=1)
  add_test(NAME EigenPartitionerCompletion COMMAND eigen_partitioner_completion_test)
  set_tests_properties(EigenPartitionerCompletion PROPERTIES TIMEOUT 60)

  foreach (partitioner IN ITEMS auto simple static affinity)
    add_test(NAME EigenPartitionerRangeOracle_${partitioner}
             COMMAND eigen_partitioner_range_test ${partitioner})
    add_test(NAME EigenPartitionerAllocationFailure_${partitioner}
             COMMAND eigen_partitioner_allocation_test ${partitioner})
    set_tests_properties(EigenPartitionerRangeOracle_${partitioner}
                         PROPERTIES TIMEOUT 120)
    set_tests_properties(EigenPartitionerAllocationFailure_${partitioner}
                         PROPERTIES TIMEOUT 60)
  endforeach ()

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

add_test(NAME NestedCMakePreservesListValues
         COMMAND ${CMAKE_COMMAND}
                 -D OOX_SOURCE_DIR=${PROJECT_SOURCE_DIR}
                 -D OOX_BINARY_DIR=${PROJECT_BINARY_DIR}
                 "-D OOX_GENERATOR=${CMAKE_GENERATOR}"
                 "-D OOX_GENERATOR_PLATFORM=${CMAKE_GENERATOR_PLATFORM}"
                 "-D OOX_GENERATOR_TOOLSET=${CMAKE_GENERATOR_TOOLSET}"
                 "-D OOX_GENERATOR_INSTANCE=${CMAKE_GENERATOR_INSTANCE}"
                 "-D OOX_MAKE_PROGRAM=${CMAKE_MAKE_PROGRAM}"
                 -P ${CMAKE_CURRENT_LIST_DIR}/nested_cmake_list_values.cmake)

if (OOX_BUILD_TWIST_TESTS)
  add_subdirectory(twist)
endif ()
