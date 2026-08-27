set(test_root "${OOX_BINARY_DIR}/tests/exported_package_consumer")
set(install_root "${test_root}/install")
set(consumer_build "${test_root}/build")
file(REMOVE_RECURSE "${test_root}")
include("${OOX_SOURCE_DIR}/tests/nested_cmake_toolchain.cmake")
oox_get_nested_cmake_args(nested_cmake_args)

set(install_command "${CMAKE_COMMAND}" --install "${OOX_BINARY_DIR}" --prefix "${install_root}")
if (OOX_BUILD_CONFIG)
  list(APPEND install_command --config "${OOX_BUILD_CONFIG}")
endif ()
execute_process(COMMAND ${install_command}
                RESULT_VARIABLE install_result
                OUTPUT_VARIABLE install_output
                ERROR_VARIABLE install_error)
if (install_result)
  message(FATAL_ERROR "OOX install failed:\n${install_output}\n${install_error}")
endif ()

execute_process(
  COMMAND "${CMAKE_COMMAND}"
          -S "${OOX_SOURCE_DIR}/tests/cmake_consumer"
          -B "${consumer_build}"
          ${nested_cmake_args}
          -D "OOX_DIR=${install_root}/${OOX_INSTALL_LIBDIR}/cmake/OOX"
  RESULT_VARIABLE configure_result
  OUTPUT_VARIABLE configure_output
  ERROR_VARIABLE configure_error)
if (configure_result)
  message(FATAL_ERROR "OOX consumer configure failed:\n${configure_output}\n${configure_error}")
endif ()

set(build_command "${CMAKE_COMMAND}" --build "${consumer_build}")
if (OOX_BUILD_CONFIG)
  list(APPEND build_command --config "${OOX_BUILD_CONFIG}")
endif ()
execute_process(COMMAND ${build_command}
                RESULT_VARIABLE build_result
                OUTPUT_VARIABLE build_output
                ERROR_VARIABLE build_error)
if (build_result)
  message(FATAL_ERROR "OOX C++17 consumer was not upgraded to C++20:\n${build_output}\n${build_error}")
endif ()
