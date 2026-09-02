set(OOX_SCHEDULER_EVAL_MODES)
set(OOX_SCHEDULER_EVAL_NON_REENTRANT_MODES)

if(OOX_ENABLE_EIGEN)
  list(APPEND OOX_SCHEDULER_EVAL_MODES
    EIGEN_STEALING EIGEN_SHARING
    EIGEN_STEALING_GRAINSIZE EIGEN_SHARING_STEALING RAPID_START
    RAPID_MAILBOX RAPID_RESIDENT RAPID_LAZY_STEALING
    RAPID_TIMESPAN_LAZY_STEALING
    # Grain-law family on the lazy coordinator (rapid_start_model.h).
    RAPID_SQRTCV_LAZY_STEALING RAPID_HEARTBEAT_LAZY_STEALING
    RAPID_FSC_LAZY_STEALING RAPID_FACTORING_LAZY_STEALING
    RAPID_GUIDED_LAZY_STEALING
    # Sqrt law with cross-call loop profile warm start.
    RAPID_TIMESPAN_LAZY_STEALING_PROFILED
    # Fixed block with alternative victim selection.
    RAPID_LAZY_STEALING_HIERARCHICAL RAPID_LAZY_STEALING_PRESSURE
    # A/B control: mailbox mode with one wake notification per task.
    RAPID_MAILBOX_PERTASK_WAKE)
  list(APPEND OOX_SCHEDULER_EVAL_NON_REENTRANT_MODES RAPID_RESIDENT)
endif()
if(TBB_FOUND)
  list(APPEND OOX_SCHEDULER_EVAL_MODES
    TBB_SIMPLE TBB_AUTO TBB_AFFINITY)
endif()
if(OpenMP_FOUND)
  list(APPEND OOX_SCHEDULER_EVAL_MODES
    OMP_STATIC OMP_DYNAMIC_NONMONOTONIC OMP_GUIDED_NONMONOTONIC)
endif()

# Map each RAPID_* evaluation mode onto its EIGEN_MODE value from modes.h.
set(oox_rapid_eigen_mode_RAPID_START EIGEN_RAPID)
set(oox_rapid_eigen_mode_RAPID_RESIDENT EIGEN_RAPID_RESIDENT)
set(oox_rapid_eigen_mode_RAPID_MAILBOX EIGEN_RAPID_MAILBOX)
set(oox_rapid_eigen_mode_RAPID_MAILBOX_PERTASK_WAKE EIGEN_RAPID_MAILBOX)
set(oox_rapid_eigen_mode_RAPID_LAZY_STEALING EIGEN_RAPID_LAZY_STEALING)
set(oox_rapid_eigen_mode_RAPID_TIMESPAN_LAZY_STEALING EIGEN_RAPID_TIMESPAN_LAZY_STEALING)
set(oox_rapid_eigen_mode_RAPID_SQRTCV_LAZY_STEALING EIGEN_RAPID_SQRTCV_LAZY)
set(oox_rapid_eigen_mode_RAPID_HEARTBEAT_LAZY_STEALING EIGEN_RAPID_HEARTBEAT_LAZY)
set(oox_rapid_eigen_mode_RAPID_FSC_LAZY_STEALING EIGEN_RAPID_FSC_LAZY)
set(oox_rapid_eigen_mode_RAPID_FACTORING_LAZY_STEALING EIGEN_RAPID_FACTORING_LAZY)
set(oox_rapid_eigen_mode_RAPID_GUIDED_LAZY_STEALING EIGEN_RAPID_GUIDED_LAZY)
set(oox_rapid_eigen_mode_RAPID_TIMESPAN_LAZY_STEALING_PROFILED EIGEN_RAPID_TIMESPAN_LAZY_PROFILED)
set(oox_rapid_eigen_mode_RAPID_LAZY_STEALING_HIERARCHICAL EIGEN_RAPID_LAZY_HIERARCHICAL)
set(oox_rapid_eigen_mode_RAPID_LAZY_STEALING_PRESSURE EIGEN_RAPID_LAZY_PRESSURE)

function(oox_configure_scheduler_eval_target target mode)
  target_include_directories(${target} PRIVATE ${CMAKE_CURRENT_LIST_DIR})
  target_compile_definitions(${target} PRIVATE
    SCHEDULER_EVAL_MODE_NAME="${mode}")
  if(mode MATCHES "^EIGEN")
    target_compile_definitions(${target} PRIVATE EIGEN_MODE=${mode})
    target_link_libraries(${target} PRIVATE OOX OOX::eigen)
  elseif(mode MATCHES "^RAPID")
    set(rapid_eigen_mode ${oox_rapid_eigen_mode_${mode}})
    if(NOT rapid_eigen_mode)
      message(FATAL_ERROR "No EIGEN_MODE mapping for scheduler eval mode ${mode}")
    endif()
    target_compile_definitions(${target} PRIVATE
      RAPID_START_MODE=1 EIGEN_MODE=${rapid_eigen_mode})
    if(mode STREQUAL "RAPID_MAILBOX_PERTASK_WAKE")
      target_compile_definitions(${target} PRIVATE OOX_EIGEN_BATCHED_WAKE=0)
    endif()
    target_link_libraries(${target} PRIVATE OOX OOX::eigen)
  elseif(mode MATCHES "^TBB")
    target_compile_definitions(${target} PRIVATE TBB_MODE=${mode})
    target_link_libraries(${target} PRIVATE OOX TBB::tbb)
  else()
    target_compile_definitions(${target} PRIVATE OMP_MODE=${mode})
    target_link_libraries(${target} PRIVATE OOX OpenMP::OpenMP_CXX)
  endif()
endfunction()

foreach(mode IN LISTS OOX_SCHEDULER_EVAL_MODES)
  set(benchmark_target bench_scheduler_eval_${mode})
  list(APPEND OOX_SCHEDULER_EVAL_TARGETS ${benchmark_target})
  add_executable(${benchmark_target} bench_scheduler_eval.cpp)
  if(NOT mode IN_LIST OOX_SCHEDULER_EVAL_NON_REENTRANT_MODES)
    target_sources(${benchmark_target} PRIVATE bench_scheduler_eval_nested.cpp)
  endif()
  oox_configure_scheduler_eval_target(${benchmark_target} ${mode})
  target_link_libraries(${benchmark_target} PRIVATE benchmark::benchmark)
  add_test(NAME ${benchmark_target}_smoke COMMAND ${benchmark_target}
    --benchmark_filter=Launch/64
    --benchmark_min_time=${OOX_BENCHMARK_CTEST_MIN_TIME})

  foreach(tool scheduling_dist trace_spin)
    set(tool_target ${tool}_${mode})
    list(APPEND OOX_SCHEDULER_EVAL_TARGETS ${tool_target})
    add_executable(${tool_target} ${tool}.cpp)
    oox_configure_scheduler_eval_target(${tool_target} ${mode})
  endforeach()
  add_test(NAME scheduling_dist_${mode}_smoke COMMAND scheduling_dist_${mode}
    --scenario spin --iterations 1 --tasks-per-worker 2)
  add_test(NAME trace_spin_${mode}_smoke COMMAND trace_spin_${mode}
    --tasks 8 --work 8 --iterations 2)

  # The tuner deliberately blocks one task per worker. Run it only with the
  # non-adaptive baseline: a timespan policy cannot split a blocked first task.
  if(mode STREQUAL "EIGEN_STEALING")
    set(tuner_target timespan_tuner_${mode})
    list(APPEND OOX_SCHEDULER_EVAL_TARGETS ${tuner_target})
    add_executable(${tuner_target} timespan_tuner.cpp)
    oox_configure_scheduler_eval_target(${tuner_target} ${mode})
    add_test(NAME ${tuner_target}_smoke COMMAND ${tuner_target} --iterations 2)
  endif()

  set(test_target test_scheduler_eval_${mode})
  list(APPEND OOX_SCHEDULER_EVAL_TARGETS ${test_target})
  add_executable(${test_target} tests.cpp)
  oox_configure_scheduler_eval_target(${test_target} ${mode})
  add_test(NAME ${test_target} COMMAND ${test_target})

  if(NOT mode IN_LIST OOX_SCHEDULER_EVAL_NON_REENTRANT_MODES)
    set(nested_test_target test_scheduler_eval_nested_${mode})
    list(APPEND OOX_SCHEDULER_EVAL_TARGETS ${nested_test_target})
    add_executable(${nested_test_target} tests_nested.cpp)
    oox_configure_scheduler_eval_target(${nested_test_target} ${mode})
    add_test(NAME ${nested_test_target} COMMAND ${nested_test_target})
  endif()
endforeach()

add_custom_target(scheduler_eval_all DEPENDS ${OOX_SCHEDULER_EVAL_TARGETS})

if(OOX_ENABLE_EIGEN OR TBB_FOUND)
  find_package(Python3 COMPONENTS Interpreter)
endif()

if(OOX_ENABLE_EIGEN AND Python3_Interpreter_FOUND)
  add_test(NAME scheduler_eval_runner_smoke
    COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_LIST_DIR}/run.py
      --build ${PROJECT_BINARY_DIR}
      --output ${CMAKE_CURRENT_BINARY_DIR}/runner-smoke
      --mode EIGEN_STEALING
      --threads 2
      --repetitions 1
      --timeout 120
      --smoke)
  set_tests_properties(scheduler_eval_runner_smoke PROPERTIES TIMEOUT 180)
endif()

if(OOX_ENABLE_EIGEN AND Python3_Interpreter_FOUND)
  add_test(NAME scheduler_eval_rapid_runner_smoke
    COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_LIST_DIR}/run.py
      --build ${PROJECT_BINARY_DIR}
      --output ${CMAKE_CURRENT_BINARY_DIR}/rapid-runner-smoke
      --mode RAPID_START
      --threads 2
      --repetitions 1
      --timeout 120
      --smoke)
  set_tests_properties(scheduler_eval_rapid_runner_smoke PROPERTIES TIMEOUT 180)
  # Exercise the fresh-process protocol end to end on one cheap policy.
  add_test(NAME scheduler_eval_fresh_process_runner_smoke
    COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_LIST_DIR}/run.py
      --build ${PROJECT_BINARY_DIR}
      --output ${CMAKE_CURRENT_BINARY_DIR}/fresh-runner-smoke
      --mode RAPID_LAZY_STEALING
      --threads 2
      --fresh-process-repetitions 2
      --shuffle-modes --seed 1
      --timeout 120
      --smoke)
  set_tests_properties(scheduler_eval_fresh_process_runner_smoke PROPERTIES TIMEOUT 240)
endif()
