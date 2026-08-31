# Copyright (C) 2021 Intel Corporation
#
# SPDX-License-Identifier: Apache-2.0
cmake_minimum_required(VERSION 3.12)

if(NOT TARGET OOX)
    project(OOX_BENCHMARKS_STANDALONE CXX)
    include(CTest)
    enable_testing()
    add_subdirectory("${CMAKE_CURRENT_SOURCE_DIR}/.." "${CMAKE_CURRENT_BINARY_DIR}/oox-root")
    return()
endif()
include(FetchContent)

if(POLICY CMP0079)
  cmake_policy(SET CMP0079 NEW)
endif()

set(BENCHMARK_ENABLE_TESTING OFF CACHE BOOL "" FORCE)
set(BENCHMARK_ENABLE_GTEST_TESTS OFF CACHE BOOL "" FORCE)
set(BENCHMARK_USE_BUNDLED_GTEST OFF CACHE BOOL "" FORCE)
set(OOX_BENCHMARK_CTEST_MIN_TIME "0.05s" CACHE STRING "Minimum Google Benchmark run time for benchmark CTest smoke tests")
set(OOX_BENCHMARK_CTEST_FILTER "/8" CACHE STRING "Google Benchmark filter used by benchmark CTest smoke tests")

FetchContent_Declare(
        benchmark
        GIT_REPOSITORY https://github.com/google/benchmark.git
        GIT_TAG v1.9.4
)
FetchContent_MakeAvailable(benchmark)

list(APPEND OOX_BENCHMARKS bench_fib bench_loops)
option(OOX_BUILD_TASKBENCH "Build Task-Bench-style benchmarks" ON)
option(OOX_BUILD_SCHEDULER_EVALS "Build composable-scheduler evaluation kernels" OFF)
set(OOX_ALLOCATOR "system" CACHE STRING "Task allocation backend for TBB OOX tasks: tbb, system, or jemalloc")
set_property(CACHE OOX_ALLOCATOR PROPERTY STRINGS tbb system jemalloc)
set(OOX_BACKENDS)

find_package(benchmark REQUIRED)

if(OOX_ALLOCATOR STREQUAL "system")
  message(STATUS "Building OOX benchmarks with system malloc task allocation")
  target_compile_definitions(${PROJECT_NAME} INTERFACE OOX_USE_STDMALLOC=1)
elseif(OOX_ALLOCATOR STREQUAL "jemalloc")
  find_library(OOX_JEMALLOC_LIBRARY
          NAMES jemalloc
          PATHS
          /opt/homebrew/opt/jemalloc/lib
          /usr/local/opt/jemalloc/lib
          /opt/homebrew/lib
          /usr/local/lib
  )
  if(NOT OOX_JEMALLOC_LIBRARY)
    message(FATAL_ERROR "OOX_ALLOCATOR=jemalloc was requested, but libjemalloc was not found")
  endif()
  message(STATUS "Building OOX benchmarks with jemalloc task allocation: ${OOX_JEMALLOC_LIBRARY}")
  target_compile_definitions(${PROJECT_NAME} INTERFACE OOX_USE_STDMALLOC=1)
  target_link_libraries(${PROJECT_NAME} INTERFACE ${OOX_JEMALLOC_LIBRARY})
elseif(NOT OOX_ALLOCATOR STREQUAL "tbb")
  message(FATAL_ERROR "Unsupported OOX_ALLOCATOR=${OOX_ALLOCATOR}; expected tbb, system, or jemalloc")
endif()

if(OpenMP_FOUND)
  message(STATUS "Building with OpenMP")
  list(APPEND OOX_BACKENDS OMP)
  list(APPEND OMP_LIBS OpenMP::OpenMP_CXX)
  list(APPEND OMP_LOOP_MODES OMP_STATIC OMP_DYNAMIC)
endif()

if(TBB_FOUND)
  message(STATUS "Building with TBB ${TBB_DIR}")
  list(APPEND OOX_BACKENDS TBB)
  list(APPEND TBB_LIBS TBB::tbb)
  if(OOX_ALLOCATOR STREQUAL "tbb")
    list(APPEND TBB_LIBS TBB::tbbmalloc)
  endif()
  list(APPEND TBB_LOOP_MODES TBB_SIMPLE TBB_AUTO TBB_AFFINITY TBB_RAPID)
endif()

if(Taskflow_FOUND)
  message(STATUS "Building with Taskflow ${Taskflow_DIR}")
  list(APPEND OOX_BACKENDS TF)
  list(APPEND TF_LIBS Taskflow::Taskflow)
  list(APPEND TF_LOOP_MODES TF_FOR_EACH)
endif()

if(folly_FOUND)
  message(STATUS "Building with Folly ${Folly_DIR}")
  list(APPEND OOX_BACKENDS FOLLY)
  list(APPEND FOLLY_LIBS folly)
  list(APPEND FOLLY_LOOP_MODES FOLLY_FOR_EACH)
endif()

if(OOX_ENABLE_EIGEN)
  message(STATUS "Building with the bundled Eigen scheduler")
  list(APPEND OOX_BACKENDS EIGEN)
  list(APPEND EIGEN_LIBS ${PROJECT_NAME}::eigen)
  list(APPEND EIGEN_LOOP_MODES
    EIGEN_STEALING_LOOP EIGEN_RAPID_LOOP EIGEN_RAPID_MAILBOX_LOOP
    EIGEN_RAPID_LAZY_STEALING_LOOP)
endif()

foreach(bench IN LISTS OOX_BENCHMARKS)
  foreach(mode IN LISTS OOX_BACKENDS)
    if("${bench}" MATCHES "loops")
      set(OOX_LOOP_MODES ${${mode}_LOOP_MODES})
      set(OOX_LOOP_CTEST_FILTER "/64")
    else()
      set(OOX_LOOP_MODES exe)
      set(OOX_LOOP_CTEST_FILTER ${OOX_BENCHMARK_CTEST_FILTER})
    endif()
    foreach(suffix IN LISTS OOX_LOOP_MODES)
      add_executable(${bench}_${mode}.${suffix} ${bench}.cpp)
      target_compile_definitions(${bench}_${mode}.${suffix} PRIVATE PARALLEL=${suffix} HAVE_${mode})
      target_link_libraries(${bench}_${mode}.${suffix} ${PROJECT_NAME} benchmark::benchmark ${${mode}_LIBS})
      target_include_directories(${bench}_${mode}.${suffix} PRIVATE ${PROJECT_NAME})
      add_test(NAME ${bench}_${mode}.${suffix}
               COMMAND ${bench}_${mode}.${suffix}
                       --benchmark_min_time=${OOX_BENCHMARK_CTEST_MIN_TIME}
                       --benchmark_filter=${OOX_LOOP_CTEST_FILTER})
    endforeach()
  endforeach()
endforeach()

foreach(mode IN LISTS OOX_BACKENDS)
  foreach(policy noexc exc)
    if(policy STREQUAL "exc")
      set(policy_value 1)
    else()
      set(policy_value 0)
    endif()
    set(fib_target bench_fib_${mode}_${policy})
    add_executable(${fib_target} bench_fib.cpp)
    target_compile_definitions(${fib_target} PRIVATE PARALLEL=exe HAVE_${mode}
      OOX_DEFAULT_EXCEPTION_POLICY=${policy_value} OOX_EXCEPTION_POLICY_STR=${policy})
    target_compile_options(${fib_target} PRIVATE -UOOX_EXCEPTIONS_ENABLED -DOOX_EXCEPTIONS_ENABLED=${policy_value})
    target_link_libraries(${fib_target} ${PROJECT_NAME} benchmark::benchmark ${${mode}_LIBS})
    target_include_directories(${fib_target} PRIVATE ${PROJECT_NAME})
    add_test(NAME ${fib_target}
             COMMAND ${fib_target} --benchmark_min_time=${OOX_BENCHMARK_CTEST_MIN_TIME} --benchmark_filter=Fib_Serial/8)
  endforeach()
endforeach()

# Disable TF benchmarks while it has deadlock due to taskflow issues
if(TBB_FOUND)
  add_executable(bench_taskbench_alt_failures bench_taskbench_alt_failures.cpp)
  target_compile_definitions(bench_taskbench_alt_failures PRIVATE
    HAVE_TBB=1
    HAVE_TF=0
  )
  target_link_libraries(bench_taskbench_alt_failures PRIVATE
    ${PROJECT_NAME}
    benchmark::benchmark
    ${TBB_LIBS}
  )
  target_include_directories(bench_taskbench_alt_failures PRIVATE
    ${PROJECT_NAME}
    ${CMAKE_CURRENT_LIST_DIR}
  )
  add_test(NAME bench_taskbench_alt_failures
           COMMAND bench_taskbench_alt_failures
                   --benchmark_min_time=${OOX_BENCHMARK_CTEST_MIN_TIME}
                   --benchmark_repetitions=1
                   --benchmark_min_warmup_time=0
                   --benchmark_filter=/128/16)
  set_tests_properties(bench_taskbench_alt_failures PROPERTIES TIMEOUT 60)
endif()

if(OOX_BUILD_TASKBENCH)
  add_subdirectory(taskbench)
endif()

if(OOX_BUILD_SCHEDULER_EVALS)
  add_subdirectory(scheduler_eval)
endif()
