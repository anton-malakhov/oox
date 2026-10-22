set(OOX_SCHEDULER_EVAL_MODES SERIAL_ELISION)
set(OOX_SCHEDULER_EVAL_NON_REENTRANT_MODES RAPID_START)
set(OOX_SCHEDULER_EVAL_TESTS)

option(OOX_SCHEDULER_EVAL_STATS "Collect Eigen task and stealing counters" ON)
option(OOX_SCHEDULER_EVAL_PAPI "Enable optional per-thread PAPI callback counters" OFF)
option(OOX_SCHEDULER_TRACE_TOOLS "Build opt-in scheduler timeline recorders" OFF)
if(OOX_SCHEDULER_TRACE_TOOLS)
  add_executable(test_scheduler_trace_recorder tracing/test_recorder.cpp tracing/recorder.cpp)
  target_link_libraries(test_scheduler_trace_recorder PRIVATE OOX)
  target_compile_definitions(test_scheduler_trace_recorder PRIVATE OOX_SCHEDULER_TRACE=1 OOX_TRACE_RECORDER_TESTING=1)
  set(OOX_SCHEDULER_TRACE_TARGETS test_scheduler_trace_recorder)
endif()
add_library(oox_eval_papi STATIC metrics/papi_metrics.cpp)
target_link_libraries(oox_eval_papi PUBLIC OOX)
if(OOX_SCHEDULER_EVAL_PAPI)
  find_path(OOX_PAPI_INCLUDE_DIR papi.h REQUIRED)
  find_library(OOX_PAPI_LIBRARY papi REQUIRED)
  target_include_directories(oox_eval_papi PRIVATE ${OOX_PAPI_INCLUDE_DIR})
  target_link_libraries(oox_eval_papi PUBLIC ${OOX_PAPI_LIBRARY})
  target_compile_definitions(oox_eval_papi PUBLIC OOX_EVAL_HAVE_PAPI=1)
endif()

if(UNIX)
  add_executable(test_eval_papi metrics/papi_metrics.cpp tests/papi/fake_papi.cpp tests/papi/tests.cpp)
  target_include_directories(test_eval_papi PRIVATE ${CMAKE_CURRENT_LIST_DIR}/tests/papi
    ${CMAKE_CURRENT_LIST_DIR}/metrics ${CMAKE_CURRENT_LIST_DIR}/runtime)
  target_compile_definitions(test_eval_papi PRIVATE OOX_EVAL_HAVE_PAPI=1 OOX_PAPI_UNIT_TEST=1)
  target_link_libraries(test_eval_papi PRIVATE OOX)
  add_test(NAME scheduler_eval_papi_lifecycle COMMAND test_eval_papi)
  list(APPEND OOX_SCHEDULER_EVAL_TARGETS test_eval_papi)
  list(APPEND OOX_SCHEDULER_EVAL_TESTS scheduler_eval_papi_lifecycle)
endif()

if(OOX_ENABLE_EIGEN)
  list(APPEND OOX_SCHEDULER_EVAL_MODES
    OOX_TASKS RAPID_GROUP RAPID_AUTO EIGEN_AUTO_RESIDENT
    EIGEN_STEALING EIGEN_SHARING
    EIGEN_AUTO EIGEN_SIMPLE EIGEN_STATIC EIGEN_AFFINITY
    EIGEN_STEALING_GRAINSIZE EIGEN_SHARING_STEALING)
endif()
if(TBB_FOUND)
  list(APPEND OOX_SCHEDULER_EVAL_MODES
    TBB_SIMPLE TBB_AUTO TBB_STATIC TBB_AFFINITY RAPID_START)
endif()
if(OpenMP_FOUND)
  list(APPEND OOX_SCHEDULER_EVAL_MODES
    OMP_STATIC OMP_DYNAMIC_NONMONOTONIC OMP_GUIDED_NONMONOTONIC)
endif()

function(oox_configure_scheduler_eval_target target mode)
  target_link_libraries(${target} PRIVATE oox_eval_papi)
  target_include_directories(${target} PRIVATE
    ${CMAKE_CURRENT_LIST_DIR}/runtime
    ${CMAKE_CURRENT_LIST_DIR}/workloads
    ${CMAKE_CURRENT_LIST_DIR}/metrics)
  target_compile_definitions(${target} PRIVATE
    OOX_EVAL_UNPINNED=1
    SCHEDULER_EVAL_MODE_NAME="${mode}")
  if(mode STREQUAL "SERIAL_ELISION")
    target_compile_definitions(${target} PRIVATE SERIAL=1)
    target_link_libraries(${target} PRIVATE OOX)
  elseif(mode STREQUAL "OOX_TASKS")
    target_compile_definitions(${target} PRIVATE
      OOX_TASK_MODE=1 $<$<BOOL:${OOX_SCHEDULER_EVAL_STATS}>:OOX_EIGEN_ENABLE_STATS=1>)
    target_link_libraries(${target} PRIVATE OOX OOX::eigen)
  elseif(mode STREQUAL "RAPID_AUTO" OR mode STREQUAL "EIGEN_AUTO_RESIDENT")
    if(mode STREQUAL "RAPID_AUTO")
      set(rapid_auto_mode EIGEN_RAPID_AUTO)
    else()
      set(rapid_auto_mode EIGEN_AUTO_RESIDENT)
    endif()
    target_compile_definitions(${target} PRIVATE RAPID_AUTO_MODE=1
      EIGEN_MODE=${rapid_auto_mode}
      $<$<BOOL:${OOX_SCHEDULER_EVAL_STATS}>:OOX_EIGEN_ENABLE_STATS=1>)
    target_link_libraries(${target} PRIVATE OOX OOX::eigen)
  elseif(mode MATCHES "^EIGEN")
    target_compile_definitions(${target} PRIVATE
      EIGEN_MODE=${mode} $<$<BOOL:${OOX_SCHEDULER_EVAL_STATS}>:OOX_EIGEN_ENABLE_STATS=1>)
    target_link_libraries(${target} PRIVATE OOX OOX::eigen)
  elseif(mode STREQUAL "RAPID_GROUP")
    target_compile_definitions(${target} PRIVATE
      RAPID_GROUP_MODE=1 EIGEN_MODE=EIGEN_RAPID_GROUP $<$<BOOL:${OOX_SCHEDULER_EVAL_STATS}>:OOX_EIGEN_ENABLE_STATS=1>)
    target_link_libraries(${target} PRIVATE OOX OOX::eigen)
  elseif(mode STREQUAL "RAPID_START")
    target_compile_definitions(${target} PRIVATE
      RAPID_START_MODE=1 TBB_MODE=TBB_RAPID)
    target_link_libraries(${target} PRIVATE OOX TBB::tbb)
  elseif(mode MATCHES "^TBB")
    target_compile_definitions(${target} PRIVATE TBB_MODE=${mode})
    target_link_libraries(${target} PRIVATE OOX TBB::tbb)
  else()
    target_compile_definitions(${target} PRIVATE OMP_MODE=${mode})
    target_link_libraries(${target} PRIVATE OOX OpenMP::OpenMP_CXX)
  endif()
endfunction()

foreach(mode IN LISTS OOX_SCHEDULER_EVAL_MODES)
  if(OOX_SCHEDULER_TRACE_TOOLS AND mode MATCHES "^(SERIAL_ELISION|RAPID_AUTO|RAPID_START|TBB_AUTO)$")
    add_executable(measure_scheduler_${mode} ${PROJECT_SOURCE_DIR}/tools/scheduler_viewer/measure_scheduler.cpp)
    oox_configure_scheduler_eval_target(measure_scheduler_${mode} ${mode})
  endif()
  if(OOX_SCHEDULER_TRACE_TOOLS AND mode MATCHES "^(RAPID_AUTO|RAPID_GROUP|RAPID_START|EIGEN_AUTO_RESIDENT|EIGEN_AUTO|EIGEN_STATIC|EIGEN_SIMPLE|EIGEN_AFFINITY|EIGEN_STEALING|EIGEN_SHARING|TBB_AUTO)$")
    add_executable(trace_scheduler_${mode} tracing/trace_scheduler.cpp tracing/recorder.cpp)
    oox_configure_scheduler_eval_target(trace_scheduler_${mode} ${mode})
    target_compile_definitions(trace_scheduler_${mode} PRIVATE OOX_SCHEDULER_TRACE=1)
    list(APPEND OOX_SCHEDULER_TRACE_TARGETS trace_scheduler_${mode})
  endif()
  set(benchmark_target bench_scheduler_eval_${mode})
  list(APPEND OOX_SCHEDULER_EVAL_TARGETS ${benchmark_target})
  add_executable(${benchmark_target} bench/bench_scheduler_eval.cpp)
  target_sources(${benchmark_target} PRIVATE
    bench/bench_primary_workloads.cpp workloads/primary_workloads.cpp
    bench/bench_extended_workloads.cpp workloads/extended_workloads.cpp
    bench/bench_synthetic_workloads.cpp workloads/synthetic_workloads.cpp
    metrics/scheduler_metrics.cpp)
  if(NOT mode IN_LIST OOX_SCHEDULER_EVAL_NON_REENTRANT_MODES)
    target_sources(${benchmark_target} PRIVATE
      bench/bench_scheduler_eval_nested.cpp
      bench/bench_graph_workloads.cpp workloads/graph_workloads.cpp workloads/graph_io.cpp workloads/granularity_control.cpp)
  endif()
  oox_configure_scheduler_eval_target(${benchmark_target} ${mode})
  target_link_libraries(${benchmark_target} PRIVATE benchmark::benchmark)
  add_test(NAME ${benchmark_target}_smoke COMMAND ${benchmark_target}
    --benchmark_filter=Launch/64
    --benchmark_min_time=${OOX_BENCHMARK_CTEST_MIN_TIME})
  list(APPEND OOX_SCHEDULER_EVAL_TESTS ${benchmark_target}_smoke)
  add_test(NAME ${benchmark_target}_extended_smoke COMMAND ${benchmark_target}
    "--benchmark_filter=(QuickHull|TrigramDedup|RadixSort64|TrigramSampleSort|SampleSortRecords|RadixPassWidth).*/1024(/|$)"
    --benchmark_min_time=${OOX_BENCHMARK_CTEST_MIN_TIME})
  list(APPEND OOX_SCHEDULER_EVAL_TESTS ${benchmark_target}_extended_smoke)
  if(mode MATCHES "^EIGEN" OR mode STREQUAL "OOX_TASKS" OR mode STREQUAL "RAPID_GROUP" OR mode STREQUAL "RAPID_AUTO")
    add_test(NAME ${benchmark_target}_availability_smoke COMMAND ${benchmark_target}
      "--benchmark_filter=(ChangingWorkerAvailability/4096|OversubscribedLoops/4096/2|FirstTouch<.*>/16384)(/|$)"
      --benchmark_min_time=${OOX_BENCHMARK_CTEST_MIN_TIME})
    list(APPEND OOX_SCHEDULER_EVAL_TESTS ${benchmark_target}_availability_smoke)
  endif()

  foreach(tool scheduling_dist trace_spin)
    set(tool_target ${tool}_${mode})
    list(APPEND OOX_SCHEDULER_EVAL_TARGETS ${tool_target})
    add_executable(${tool_target} probes/${tool}.cpp)
    oox_configure_scheduler_eval_target(${tool_target} ${mode})
  endforeach()
  add_test(NAME scheduling_dist_${mode}_smoke COMMAND scheduling_dist_${mode}
    --scenario spin --iterations 1 --tasks-per-worker 2)
  add_test(NAME trace_spin_${mode}_smoke COMMAND trace_spin_${mode}
    --tasks 8 --work 8 --iterations 2)
  list(APPEND OOX_SCHEDULER_EVAL_TESTS
    scheduling_dist_${mode}_smoke trace_spin_${mode}_smoke)

  # The tuner deliberately blocks one task per worker. Run it only with the
  # non-adaptive baseline: a timespan policy cannot split a blocked first task.
  if(mode STREQUAL "EIGEN_STEALING")
    set(tuner_target timespan_tuner_${mode})
    list(APPEND OOX_SCHEDULER_EVAL_TARGETS ${tuner_target})
    add_executable(${tuner_target} probes/timespan_tuner.cpp)
    oox_configure_scheduler_eval_target(${tuner_target} ${mode})
    add_test(NAME ${tuner_target}_smoke COMMAND ${tuner_target} --iterations 2)
    list(APPEND OOX_SCHEDULER_EVAL_TESTS ${tuner_target}_smoke)
  endif()

  set(test_target test_scheduler_eval_${mode})
  list(APPEND OOX_SCHEDULER_EVAL_TARGETS ${test_target})
  add_executable(${test_target} tests/tests.cpp)
  target_sources(${test_target} PRIVATE
    workloads/primary_workloads.cpp workloads/synthetic_workloads.cpp
    workloads/extended_workloads.cpp workloads/granularity_control.cpp tests/tests_extended.cpp)
  if(NOT mode IN_LIST OOX_SCHEDULER_EVAL_NON_REENTRANT_MODES)
    target_sources(${test_target} PRIVATE
      workloads/graph_workloads.cpp workloads/graph_io.cpp)
    target_compile_definitions(${test_target} PRIVATE
      SCHEDULER_EVAL_HAS_NESTED_BFS=1)
  endif()
  oox_configure_scheduler_eval_target(${test_target} ${mode})
  add_test(NAME ${test_target} COMMAND ${test_target})
  list(APPEND OOX_SCHEDULER_EVAL_TESTS ${test_target})

  if(NOT mode IN_LIST OOX_SCHEDULER_EVAL_NON_REENTRANT_MODES)
    set(nested_test_target test_scheduler_eval_nested_${mode})
    list(APPEND OOX_SCHEDULER_EVAL_TARGETS ${nested_test_target})
    add_executable(${nested_test_target} tests/tests_nested.cpp)
    oox_configure_scheduler_eval_target(${nested_test_target} ${mode})
    add_test(NAME ${nested_test_target} COMMAND ${nested_test_target})
    list(APPEND OOX_SCHEDULER_EVAL_TESTS ${nested_test_target})
  endif()
endforeach()

# Keep the explicit-policy experiment opt-in: do not multiply the full-suite CI
# matrix. Both sides reuse the calibration workload and the same resident pool.
if(OOX_SCHEDULER_TRACE_TOOLS AND OOX_ENABLE_EIGEN)
  foreach(policy IN ITEMS Auto Simple Static Affinity)
    string(TOUPPER "${policy}" policy_name)
    foreach(activate IN ITEMS 0 1)
      if(activate)
        set(probe_mode RAPID_NATIVE_${policy_name})
      else()
        set(probe_mode NATIVE_${policy_name}_RESIDENT)
      endif()
      add_executable(measure_scheduler_${probe_mode} ${PROJECT_SOURCE_DIR}/tools/scheduler_viewer/measure_scheduler.cpp)
      set(policy_targets measure_scheduler_${probe_mode})
      if(activate)
        add_executable(trace_scheduler_${probe_mode} tracing/trace_scheduler.cpp tracing/recorder.cpp)
        target_compile_definitions(trace_scheduler_${probe_mode} PRIVATE OOX_SCHEDULER_TRACE=1)
        list(APPEND policy_targets trace_scheduler_${probe_mode})
        list(APPEND OOX_SCHEDULER_TRACE_TARGETS trace_scheduler_${probe_mode})
      endif()
      foreach(policy_target IN LISTS policy_targets)
        target_link_libraries(${policy_target} PRIVATE OOX OOX::eigen oox_eval_papi)
        target_include_directories(${policy_target} PRIVATE
          ${CMAKE_CURRENT_LIST_DIR}/runtime ${CMAKE_CURRENT_LIST_DIR}/metrics)
        target_compile_definitions(${policy_target} PRIVATE
          OOX_EVAL_UNPINNED=1 EIGEN_MODE=EIGEN_RAPID_AUTO
          SCHEDULER_EVAL_MODE_NAME="${probe_mode}"
          OOX_RAPID_POLICY=${policy}Partitioner OOX_RAPID_POLICY_ACTIVATE=${activate})
      endforeach()
    endforeach()
  endforeach()
endif()

add_executable(test_eval_arguments tests/tests_arguments.cpp)
oox_configure_scheduler_eval_target(test_eval_arguments SERIAL_ELISION)
list(APPEND OOX_SCHEDULER_EVAL_TARGETS test_eval_arguments)
add_custom_target(scheduler_eval_all DEPENDS ${OOX_SCHEDULER_EVAL_TARGETS})
if(OOX_SCHEDULER_TRACE_TOOLS)
  add_custom_target(scheduler_trace_all DEPENDS ${OOX_SCHEDULER_TRACE_TARGETS})
endif()

if(OOX_ENABLE_EIGEN OR TBB_FOUND OR OOX_SCHEDULER_TRACE_TOOLS)
  find_package(Python3 COMPONENTS Interpreter)
endif()

if(OOX_SCHEDULER_TRACE_TOOLS)
  if(Python3_Interpreter_FOUND)
    add_test(NAME scheduler_viewer_python COMMAND ${Python3_EXECUTABLE} -B -m unittest discover
      -s ${PROJECT_SOURCE_DIR}/tools/scheduler_viewer -p "test_*.py")
    set_tests_properties(scheduler_viewer_python PROPERTIES LABELS scheduler-viewer TIMEOUT 60)
  endif()
  if(OOX_ENABLE_EIGEN AND TBB_FOUND AND Python3_Interpreter_FOUND)
    add_test(NAME scheduler_trace_protocol
      COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_LIST_DIR}/tracing/test_trace.py ${PROJECT_BINARY_DIR})
    add_test(NAME scheduler_trace_audit
      COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_LIST_DIR}/tracing/test_audit.py)
    set_tests_properties(scheduler_trace_protocol PROPERTIES TIMEOUT 180)
  endif()
  find_program(OOX_TRACE_NODE_EXECUTABLE node)
  if(OOX_TRACE_NODE_EXECUTABLE)
    foreach(check IN ITEMS model native_model trace_state trace_ui flowchart lab lab_ui)
      add_test(NAME scheduler_viewer_${check} COMMAND ${OOX_TRACE_NODE_EXECUTABLE}
        ${PROJECT_SOURCE_DIR}/tools/scheduler_viewer/test_${check}.cjs)
      set_tests_properties(scheduler_viewer_${check} PROPERTIES LABELS scheduler-viewer TIMEOUT 60)
    endforeach()
    add_test(NAME scheduler_trace_viewer_model
      COMMAND ${OOX_TRACE_NODE_EXECUTABLE} ${CMAKE_CURRENT_LIST_DIR}/tracing/test_model.js)
  endif()
endif()

if(OOX_ENABLE_EIGEN AND Python3_Interpreter_FOUND)
  add_test(NAME scheduler_eval_arguments
    COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_LIST_DIR}/tests/tests_arguments.py $<TARGET_FILE:test_eval_arguments>)
  add_test(NAME scheduler_eval_process_cleanup
    COMMAND ${Python3_EXECUTABLE} ${PROJECT_SOURCE_DIR}/benchmarks/pbbs/tests_processes.py)
  add_test(NAME scheduler_eval_shell_safety
    COMMAND ${Python3_EXECUTABLE} ${PROJECT_SOURCE_DIR}/scripts/tests_benchmark_scripts.py)
  list(APPEND OOX_SCHEDULER_EVAL_TESTS scheduler_eval_arguments scheduler_eval_process_cleanup scheduler_eval_shell_safety)
  add_test(NAME scheduler_eval_pbbs_snapshot
    COMMAND ${Python3_EXECUTABLE} ${PROJECT_SOURCE_DIR}/benchmarks/pbbs/tests_snapshot.py)
  list(APPEND OOX_SCHEDULER_EVAL_TESTS scheduler_eval_pbbs_snapshot)
  add_test(NAME scheduler_eval_dataset_integrity
    COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_LIST_DIR}/tests/tests_datasets.py)
  list(APPEND OOX_SCHEDULER_EVAL_TESTS scheduler_eval_dataset_integrity)
  add_test(NAME scheduler_eval_historical_tools
    COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_LIST_DIR}/tests/tests_tools.py)
  list(APPEND OOX_SCHEDULER_EVAL_TESTS scheduler_eval_historical_tools)
  add_test(NAME scheduler_eval_serial_runner_smoke
    COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_LIST_DIR}/run.py
      --build ${PROJECT_BINARY_DIR}
      --output ${CMAKE_CURRENT_BINARY_DIR}/serial-runner-smoke
      --mode SERIAL_ELISION --threads 2 --repetitions 1 --timeout 120 --smoke)
  set_tests_properties(scheduler_eval_serial_runner_smoke PROPERTIES TIMEOUT 180)
  list(APPEND OOX_SCHEDULER_EVAL_TESTS scheduler_eval_serial_runner_smoke)
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
  list(APPEND OOX_SCHEDULER_EVAL_TESTS scheduler_eval_runner_smoke)
  add_test(NAME scheduler_eval_graph_runner_smoke
    COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_LIST_DIR}/run.py
      --build ${PROJECT_BINARY_DIR}
      --output ${CMAKE_CURRENT_BINARY_DIR}/graph-runner-smoke
      --mode EIGEN_STEALING --threads 2 --repetitions 1 --timeout 120
      --graph ${CMAKE_CURRENT_LIST_DIR}/tests/fixtures/graph_smoke.adj
      --source-vertex 2
      --filter BfsFile --smoke)
  set_tests_properties(scheduler_eval_graph_runner_smoke PROPERTIES TIMEOUT 180)
  list(APPEND OOX_SCHEDULER_EVAL_TESTS scheduler_eval_graph_runner_smoke)
endif()

if(TBB_FOUND AND Python3_Interpreter_FOUND)
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
  list(APPEND OOX_SCHEDULER_EVAL_TESTS scheduler_eval_rapid_runner_smoke)
endif()

if(OOX_SCHEDULER_EVAL_TESTS)
  set_tests_properties(${OOX_SCHEDULER_EVAL_TESTS}
    PROPERTIES LABELS scheduler-eval
      FAIL_REGULAR_EXPRESSION "Failed to match any benchmarks|ERROR OCCURRED")
endif()
