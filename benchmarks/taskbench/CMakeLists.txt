foreach(mode IN LISTS OOX_BACKENDS)
  add_executable(bench_taskbench_${mode}.exe bench_taskbench.cpp)
  target_compile_definitions(bench_taskbench_${mode}.exe PRIVATE HAVE_${mode})
  target_link_libraries(bench_taskbench_${mode}.exe PRIVATE ${PROJECT_NAME} benchmark::benchmark ${${mode}_LIBS})
  target_include_directories(bench_taskbench_${mode}.exe PRIVATE
          ${CMAKE_CURRENT_LIST_DIR}
          ${PROJECT_SOURCE_DIR}
  )
  add_test(NAME bench_taskbench_${mode}_stencil_smoke
           COMMAND bench_taskbench_${mode}.exe
                   --height=4
                   --width=4
                   --pattern=stencil
                   --iterations=100
                   --repetitions=1
                   --warmups=0
                   --validate=1
                   --csv)
  add_test(NAME bench_taskbench_${mode}_trivial_smoke
           COMMAND bench_taskbench_${mode}.exe
                   --height=4
                   --width=4
                   --pattern=trivial
                   --kernel=empty
                   --iterations=100
                   --repetitions=1
                   --warmups=0
                   --validate=1
                   --csv)
  if("${mode}" STREQUAL "TF")
    add_test(NAME bench_taskbench_${mode}_direct_taskflow_smoke
             COMMAND bench_taskbench_${mode}.exe
                     --runner=taskflow
                     --height=4
                     --width=4
                     --pattern=stencil
                     --iterations=100
                     --repetitions=1
                     --warmups=0
                     --validate=1
                     --csv)
  elseif("${mode}" STREQUAL "TBB")
    add_test(NAME bench_taskbench_${mode}_direct_tbb_flow_smoke
             COMMAND bench_taskbench_${mode}.exe
                     --runner=tbb-flow
                     --height=4
                     --width=4
                     --pattern=stencil
                     --iterations=100
                     --repetitions=1
                     --warmups=0
                     --validate=1
                     --csv)
  elseif("${mode}" STREQUAL "OMP")
    add_test(NAME bench_taskbench_${mode}_direct_openmp_smoke
             COMMAND bench_taskbench_${mode}.exe
                     --runner=openmp
                     --height=4
                     --width=4
                     --pattern=stencil
                     --iterations=100
                     --repetitions=1
                     --warmups=0
                     --validate=1
                     --csv)
  elseif("${mode}" STREQUAL "FOLLY")
    add_test(NAME bench_taskbench_${mode}_direct_folly_smoke
             COMMAND bench_taskbench_${mode}.exe
                     --runner=folly
                     --height=4
                     --width=4
                     --pattern=stencil
                     --iterations=1
                     --repetitions=1
                     --warmups=0
                     --validate=1
                     --csv)
  endif()
endforeach()

if(BUILD_TESTING)
  add_executable(test_taskbench_patterns tests/test_patterns.cpp)
  target_link_libraries(test_taskbench_patterns PRIVATE ${PROJECT_NAME})
  target_include_directories(test_taskbench_patterns PRIVATE ${CMAKE_CURRENT_LIST_DIR} ${PROJECT_SOURCE_DIR})
  add_test(NAME test_taskbench_patterns COMMAND test_taskbench_patterns)

  add_executable(test_taskbench_metrics tests/test_metrics.cpp)
  target_link_libraries(test_taskbench_metrics PRIVATE ${PROJECT_NAME})
  target_include_directories(test_taskbench_metrics PRIVATE ${CMAKE_CURRENT_LIST_DIR} ${PROJECT_SOURCE_DIR})
  add_test(NAME test_taskbench_metrics COMMAND test_taskbench_metrics)
endif()
