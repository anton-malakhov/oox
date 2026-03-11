# Copyright (C) 2021 Intel Corporation
#
# SPDX-License-Identifier: Apache-2.0

include(FetchContent)

set(BENCHMARK_ENABLE_TESTING OFF CACHE BOOL "" FORCE)
set(BENCHMARK_ENABLE_GTEST_TESTS OFF CACHE BOOL "" FORCE)
set(BENCHMARK_USE_BUNDLED_GTEST OFF CACHE BOOL "" FORCE)

FetchContent_Declare(
        benchmark
        GIT_REPOSITORY https://github.com/google/benchmark.git
        GIT_TAG v1.9.4
)
FetchContent_MakeAvailable(benchmark)

list(APPEND OOX_BENCHMARKS bench_fib bench_loops)
set(OOX_BACKENDS)

find_package(benchmark REQUIRED)

if(OpenMP_FOUND)
  message(STATUS "Building with OpenMP")
  list(APPEND OOX_BACKENDS OMP)
  list(APPEND OMP_LIBS OpenMP::OpenMP_CXX)
  list(APPEND OMP_LOOP_MODES OMP_STATIC OMP_DYNAMIC)
endif()

if(TBB_FOUND)
  message(STATUS "Building with TBB ${TBB_DIR}")
  list(APPEND OOX_BACKENDS TBB)
  list(APPEND TBB_LIBS TBB::tbb TBB::tbbmalloc)
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

foreach(bench IN LISTS OOX_BENCHMARKS)
  foreach(mode IN LISTS OOX_BACKENDS)
    if("${bench}" MATCHES "loops")
      set(OOX_LOOP_MODES ${${mode}_LOOP_MODES})
    else()
      set(OOX_LOOP_MODES exe)
    endif()
    foreach(suffix IN LISTS OOX_LOOP_MODES)
      add_executable(${bench}_${mode}.${suffix} ${bench}.cpp)
      target_compile_definitions(${bench}_${mode}.${suffix} PRIVATE PARALLEL=${suffix} HAVE_${mode})
      target_link_libraries(${bench}_${mode}.${suffix} ${PROJECT_NAME} benchmark::benchmark ${${mode}_LIBS})
      target_include_directories(${bench}_${mode}.${suffix} PRIVATE ${PROJECT_NAME})
      add_test(${bench}_${mode}.${suffix} ${bench}_${mode}.${suffix} --benchmark_min_time=0 --benchmark_repetitions=1  --benchmark_min_warmup_time=0 --benchmark_filter=/8)
    endforeach()
  endforeach()
endforeach()
