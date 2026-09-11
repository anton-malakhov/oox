if (NOT OOX_SOURCE_DIR OR NOT OOX_BINARY_DIR)
  message(FATAL_ERROR "OOX_SOURCE_DIR and OOX_BINARY_DIR are required")
endif ()

set(test_source_dir "${OOX_BINARY_DIR}/nonthrow_consume_compile_src")
set(test_build_dir "${OOX_BINARY_DIR}/nonthrow_consume_compile_build")
file(REMOVE_RECURSE "${test_build_dir}")
file(MAKE_DIRECTORY "${test_source_dir}")
include("${OOX_SOURCE_DIR}/tests/nested_cmake_toolchain.cmake")
oox_get_nested_cmake_args(nested_cmake_args)

file(WRITE "${test_source_dir}/CMakeLists.txt" [=[
cmake_minimum_required(VERSION 3.14)
project(NonThrowConsumeCompile LANGUAGES CXX)
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_TRY_COMPILE_TARGET_TYPE STATIC_LIBRARY)

function(expect_compile case_name expected macro_value)
  try_compile(result
              "${CMAKE_BINARY_DIR}/${case_name}_${macro_value}"
              "${OOX_SOURCE_DIR}/tests/compile_cases/${case_name}.cpp"
              COMPILE_DEFINITIONS
                "-I${OOX_SOURCE_DIR}"
                "-DOOX_EXCEPTIONS_ENABLED=${macro_value}"
              OUTPUT_VARIABLE output)
  if (expected AND NOT result)
    message(FATAL_ERROR "Expected ${case_name} with exceptions=${macro_value} to compile:\n${output}")
  elseif (NOT expected AND result)
    message(FATAL_ERROR "Expected ${case_name} with exceptions=${macro_value} to fail")
  endif ()
endfunction()

function(expect_no_compiler_exceptions_compile)
  if (MSVC)
    set(no_exceptions_flag "/EHs-c-")
  else ()
    set(no_exceptions_flag "-fno-exceptions")
  endif ()
  try_compile(result
              "${CMAKE_BINARY_DIR}/shared_var_no_compiler_exceptions"
              "${OOX_SOURCE_DIR}/tests/compile_cases/shared_var_no_compiler_exceptions.cpp"
              COMPILE_DEFINITIONS
                "-I${OOX_SOURCE_DIR}"
                "-DOOX_EXCEPTIONS_ENABLED=0"
                "${no_exceptions_flag}"
              OUTPUT_VARIABLE output)
  if (NOT result)
    message(FATAL_ERROR "Expected shared_var to compile without compiler exceptions:\n${output}")
  endif ()
endfunction()

foreach (macro_value 0 1)
  expect_compile(nonthrow_consume_ok TRUE ${macro_value})
  expect_compile(nonthrow_consume_fail_default FALSE ${macro_value})
  expect_compile(nonthrow_consume_fail_copy FALSE ${macro_value})
  expect_compile(nonthrow_consume_fail_cross_type FALSE ${macro_value})
  expect_compile(shared_var_fail_immovable FALSE ${macro_value})
endforeach ()

expect_no_compiler_exceptions_compile()
]=])

execute_process(COMMAND ${CMAKE_COMMAND}
                        -S "${test_source_dir}"
                        -B "${test_build_dir}"
                        ${nested_cmake_args}
                        -D "OOX_SOURCE_DIR=${OOX_SOURCE_DIR}"
                RESULT_VARIABLE configure_result)
if (configure_result)
  message(FATAL_ERROR "Non-throwing consume compile checks failed")
endif ()
