# Copyright (C) 2021 Intel Corporation
#
# SPDX-License-Identifier: Apache-2.0

include(FetchContent)

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

list(APPEND OOX_BENCHMARKS bench_fib bench_loops bench_exceptions bench_cancellation bench_success_graphs)
set(OOX_BACKENDS)

find_package(benchmark REQUIRED)

if(OpenMP_FOUND)
  message(STATUS "Building with OpenMP")
  list(APPEND OOX_BACKENDS OMP)
  list(APPEND OMP_LIBS OpenMP::OpenMP_CXX)
  list(APPEND OMP_LOOP_MODES OMP_STATIC OMP_DYNAMIC)
  list(APPEND OMP_EXCEPTION_MODES OMP_STATIC OMP_DYNAMIC)
endif()

if(TBB_FOUND)
  message(STATUS "Building with TBB ${TBB_DIR}")
  list(APPEND OOX_BACKENDS TBB)
  list(APPEND TBB_LIBS TBB::tbb)
  set(TBB_BENCHMARK_LIBS benchmark::benchmark ${TBB_LIBS})
  list(APPEND TBB_LOOP_MODES TBB_SIMPLE TBB_AUTO TBB_AFFINITY TBB_RAPID)
  list(APPEND TBB_EXCEPTION_MODES TBB_SIMPLE TBB_AUTO TBB_AFFINITY TBB_RAPID)
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
  list(APPEND FOLLY_EXCEPTION_MODES FOLLY_FOR_EACH)
endif()

foreach(bench IN LISTS OOX_BENCHMARKS)
  foreach(mode IN LISTS OOX_BACKENDS)
    if("${bench}" MATCHES "loops")
      set(OOX_LOOP_MODES ${${mode}_LOOP_MODES})
    elseif ("${bench}" MATCHES "exceptions")
      set(OOX_LOOP_MODES ${${mode}_EXCEPTION_MODES})
    else()
      set(OOX_LOOP_MODES exe)
    endif()
    foreach(suffix IN LISTS OOX_LOOP_MODES)
      if("${bench}" STREQUAL "bench_cancellation")
        if(OOX_EXCEPTIONS_ENABLED)
          set(OOX_POLICY_LIST exc)
        else()
          set(OOX_POLICY_LIST)
        endif()
      elseif("${bench}" STREQUAL "bench_exceptions" AND NOT OOX_EXCEPTIONS_ENABLED)
        set(OOX_POLICY_LIST noexc)
      else()
        set(OOX_POLICY_LIST noexc exc)
      endif()
      foreach(policy IN LISTS OOX_POLICY_LIST)
        if(policy STREQUAL "exc")
          set(OOX_POLICY_VALUE 1)
        else()
          set(OOX_POLICY_VALUE 0)
        endif()
        if("${suffix}" STREQUAL "exe")
          set(bench_target ${bench}_${mode}.${policy})
        else()
          set(bench_target ${bench}_${mode}.${suffix}_${policy})
        endif()
        add_executable(${bench_target} ${bench}.cpp)
        target_compile_definitions(${bench_target} PRIVATE PARALLEL=${suffix} HAVE_${mode}
                                   OOX_DEFAULT_EXCEPTION_POLICY=${OOX_POLICY_VALUE}
                                   OOX_EXCEPTION_POLICY_STR=${policy})
        target_compile_options(${bench_target} PRIVATE
                                -UOOX_EXCEPTIONS_ENABLED
                                -DOOX_EXCEPTIONS_ENABLED=${OOX_POLICY_VALUE})
        if ("${mode}" STREQUAL "TBB")
          if (OOX_POLICY_VALUE)
            target_compile_definitions(${bench_target} PRIVATE TBB_USE_EXCEPTIONS=1)
            target_compile_options(${bench_target} PRIVATE -fexceptions)
          endif()
        endif()
        target_link_libraries(${bench_target} ${PROJECT_NAME} benchmark::benchmark ${${mode}_LIBS})
        target_include_directories(${bench_target} PRIVATE ${PROJECT_NAME})
        # CI test filter
        if(bench STREQUAL "bench_fib")
          set(_bench_ci_filter "^Fib_(Serial|OOX[12]|TBB[12]|TF|OMP|TokenCancel|Throw_Start|Cancel_Start)/4/")
        else()
          set(_bench_ci_filter "/4")
        endif()
        add_test(${bench_target} ${bench_target}
          --benchmark_min_time=${OOX_BENCHMARK_CTEST_MIN_TIME}
          --benchmark_repetitions=1
          --benchmark_min_warmup_time=0
          --benchmark_filter=${_bench_ci_filter})
      endforeach()
    endforeach()
  endforeach()
endforeach()

if(TBB_FOUND)
  # Standalone TBB exception-overhead benchmark (does not use OOX).
  add_executable(bench_tbb_exceptions_noexc bench_tbb_exceptions.cpp)
  target_compile_definitions(bench_tbb_exceptions_noexc PRIVATE
    HAVE_TBB
    TBB_USE_EXCEPTIONS=0
    TBB_EXCEPTION_POLICY_STR=noexc
  )
  target_compile_options(bench_tbb_exceptions_noexc PRIVATE -fno-exceptions)
  target_link_libraries(bench_tbb_exceptions_noexc PRIVATE ${TBB_BENCHMARK_LIBS})
  add_test(bench_tbb_exceptions_noexc
    bench_tbb_exceptions_noexc --benchmark_min_time=0 --benchmark_repetitions=1 --benchmark_min_warmup_time=0 --benchmark_filter=NoThrow)

  add_executable(bench_tbb_exceptions_exc bench_tbb_exceptions.cpp)
  target_compile_definitions(bench_tbb_exceptions_exc PRIVATE
    HAVE_TBB
    TBB_USE_EXCEPTIONS=1
    TBB_EXCEPTION_POLICY_STR=exc
  )
  target_compile_options(bench_tbb_exceptions_exc PRIVATE -fexceptions)
  target_link_libraries(bench_tbb_exceptions_exc PRIVATE ${TBB_BENCHMARK_LIBS})
  add_test(bench_tbb_exceptions_exc
    bench_tbb_exceptions_exc --benchmark_min_time=0 --benchmark_repetitions=1 --benchmark_min_warmup_time=0 --benchmark_filter=NoThrow)
endif()

if(TBB_FOUND AND OOX_EXCEPTIONS_ENABLED)
  add_executable(bench_branching_cancel bench_branching_cancel.cpp)
  target_compile_definitions(bench_branching_cancel PRIVATE
    HAVE_TBB
    OOX_DEFAULT_EXCEPTION_POLICY=1
    OOX_EXCEPTION_POLICY_STR=exc
    TBB_USE_EXCEPTIONS=1
  )
  target_compile_options(bench_branching_cancel PRIVATE
    -UOOX_EXCEPTIONS_ENABLED
    -DOOX_EXCEPTIONS_ENABLED=1
    -fexceptions
  )
  target_link_libraries(bench_branching_cancel PRIVATE ${PROJECT_NAME} ${TBB_BENCHMARK_LIBS})
  target_include_directories(bench_branching_cancel PRIVATE ${PROJECT_NAME})
  add_test(bench_branching_cancel
    bench_branching_cancel --benchmark_min_time=${OOX_BENCHMARK_CTEST_MIN_TIME} --benchmark_repetitions=1 --benchmark_min_warmup_time=0 --benchmark_filter=/10)
endif()
