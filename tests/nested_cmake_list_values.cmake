if (NOT OOX_SOURCE_DIR OR NOT OOX_BINARY_DIR OR NOT OOX_GENERATOR)
  message(FATAL_ERROR "OOX_SOURCE_DIR, OOX_BINARY_DIR, and OOX_GENERATOR are required")
endif ()

set(test_root "${OOX_BINARY_DIR}/nested_cmake_list_values")
set(test_source_dir "${test_root}/src")
set(test_build_dir "${test_root}/build")
file(REMOVE_RECURSE "${test_root}")
file(MAKE_DIRECTORY "${test_source_dir}")

file(WRITE "${test_source_dir}/CMakeLists.txt" [=[
cmake_minimum_required(VERSION 3.14)
project(NestedCMakeListValues NONE)
if (NOT "${CMAKE_OSX_ARCHITECTURES}" STREQUAL "x86_64;arm64")
  message(FATAL_ERROR
          "Nested CMAKE_OSX_ARCHITECTURES was '${CMAKE_OSX_ARCHITECTURES}'")
endif ()
]=])

set(OOX_OSX_ARCHITECTURES "x86_64;arm64")
include("${OOX_SOURCE_DIR}/tests/nested_cmake_toolchain.cmake")
oox_get_nested_cmake_args(nested_cmake_args)

execute_process(
  COMMAND "${CMAKE_COMMAND}"
          -S "${test_source_dir}"
          -B "${test_build_dir}"
          ${nested_cmake_args}
  RESULT_VARIABLE configure_result
  OUTPUT_VARIABLE configure_output
  ERROR_VARIABLE configure_error)
if (configure_result)
  message(FATAL_ERROR
          "Nested configure did not preserve a list-valued cache setting:\n"
          "${configure_output}\n${configure_error}")
endif ()
