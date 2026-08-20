set(install_dir "${OOX_BINARY_DIR}/package-consumer-install")
set(consumer_build_dir "${OOX_BINARY_DIR}/package-consumer-build")

set(install_command
    "${CMAKE_COMMAND}" --install "${OOX_BINARY_DIR}" --prefix "${install_dir}")
if (OOX_MULTI_CONFIG)
  list(APPEND install_command --config "${OOX_CONFIG}")
endif ()
execute_process(COMMAND ${install_command} RESULT_VARIABLE install_result)
if (NOT install_result EQUAL 0)
  message(FATAL_ERROR "OOX install failed: ${install_result}")
endif ()

set(package_dir "${install_dir}/${OOX_PACKAGE_DIR}")
if (NOT EXISTS "${package_dir}/OOXConfig.cmake")
  message(FATAL_ERROR "OOX package config was not installed to ${package_dir}")
endif ()

execute_process(
  COMMAND "${CMAKE_COMMAND}"
          -S "${OOX_SOURCE_DIR}/tests/package_consumer"
          -B "${consumer_build_dir}"
          -G "${OOX_GENERATOR}"
          -D "OOX_DIR=${package_dir}"
          -D "CMAKE_CXX_COMPILER=${OOX_CXX_COMPILER}"
          -D CMAKE_CXX_EXTENSIONS=OFF
  RESULT_VARIABLE configure_result)
if (NOT configure_result EQUAL 0)
  message(FATAL_ERROR "OOX package consumer configure failed: ${configure_result}")
endif ()

set(build_command "${CMAKE_COMMAND}" --build "${consumer_build_dir}")
if (OOX_MULTI_CONFIG)
  list(APPEND build_command --config "${OOX_CONFIG}")
endif ()
execute_process(COMMAND ${build_command} RESULT_VARIABLE build_result)
if (NOT build_result EQUAL 0)
  message(FATAL_ERROR "OOX package consumer build failed: ${build_result}")
endif ()

set(executable_dir "${consumer_build_dir}")
if (OOX_MULTI_CONFIG)
  set(executable_dir "${executable_dir}/${OOX_CONFIG}")
endif ()
execute_process(
  COMMAND "${executable_dir}/oox_eigen_consumer${OOX_EXE_SUFFIX}"
  RESULT_VARIABLE run_result)
if (NOT run_result EQUAL 0)
  message(FATAL_ERROR "OOX package consumer failed: ${run_result}")
endif ()
