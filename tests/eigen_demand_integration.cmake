# SPDX-License-Identifier: Apache-2.0
set(stage "${BINARY_DIR}/demand-install")
execute_process(COMMAND "${CMAKE_COMMAND}" --install "${BINARY_DIR}" --prefix "${stage}"
                RESULT_VARIABLE result OUTPUT_QUIET ERROR_VARIABLE error)
if (NOT result EQUAL 0)
  message(FATAL_ERROR "Install failed: ${error}")
endif ()
set(consumer "${BINARY_DIR}/demand-consumer")
file(MAKE_DIRECTORY "${consumer}")
file(WRITE "${consumer}/main.cpp" "#include <oox/oox.h>\nint main() { auto x = oox::run([]() noexcept { return 7; }); return oox::wait_and_get(x) != 7; }\n")
file(WRITE "${consumer}/CMakeLists.txt" "
cmake_minimum_required(VERSION 3.18)
project(DemandConsumer LANGUAGES CXX)
find_package(OOX CONFIG REQUIRED)
add_executable(consumer main.cpp)
target_link_libraries(consumer PRIVATE OOX::eigen_demand)
")
execute_process(COMMAND "${CMAKE_COMMAND}" -S "${consumer}" -B "${consumer}/build"
                "-DCMAKE_PREFIX_PATH=${stage}" "-DCMAKE_CXX_COMPILER=${COMPILER}"
                "-DCMAKE_CXX_FLAGS=${FLAGS}"
                RESULT_VARIABLE result OUTPUT_QUIET ERROR_VARIABLE error)
if (NOT result EQUAL 0)
  message(FATAL_ERROR "Installed consumer configure failed: ${error}")
endif ()
execute_process(COMMAND "${CMAKE_COMMAND}" --build "${consumer}/build"
                RESULT_VARIABLE result OUTPUT_QUIET ERROR_VARIABLE error)
if (NOT result EQUAL 0)
  message(FATAL_ERROR "Installed consumer build failed: ${error}")
endif ()
execute_process(COMMAND "${consumer}/build/consumer" RESULT_VARIABLE result TIMEOUT 15)
if (NOT result EQUAL 0)
  message(FATAL_ERROR "Installed consumer failed: ${result}")
endif ()

# Both Eigen variants must be rejected rather than selected by include order.
execute_process(COMMAND "${COMPILER}" -std=c++20 "-I${SOURCE_DIR}"
                -DHAVE_EIGEN=1 -DHAVE_EIGEN_DEMAND=1 -fsyntax-only
                "${consumer}/main.cpp"
                RESULT_VARIABLE result OUTPUT_VARIABLE output ERROR_VARIABLE error)
if (result EQUAL 0 OR NOT error MATCHES "Enable exactly one OOX asynchronous backend")
  message(FATAL_ERROR "Mixed backend selection was not rejected: ${output}${error}")
endif ()

# Serial debugging intentionally takes precedence over asynchronous choices.
execute_process(COMMAND "${COMPILER}" -std=c++20 "-I${SOURCE_DIR}"
                -DHAVE_EIGEN=1 -DHAVE_EIGEN_DEMAND=1 -DOOX_SERIAL_DEBUG=1
                -fsyntax-only "${consumer}/main.cpp"
                RESULT_VARIABLE result OUTPUT_QUIET ERROR_VARIABLE error)
if (NOT result EQUAL 0)
  message(FATAL_ERROR "Serial override failed: ${error}")
endif ()
