if (NOT OOX_SOURCE_DIR OR NOT OOX_BINARY_DIR)
  message(FATAL_ERROR "OOX_SOURCE_DIR and OOX_BINARY_DIR are required")
endif()

set(OOX_TRY_SRC_DIR "${OOX_BINARY_DIR}/exception_policy_compile_src")
set(OOX_TRY_BUILD_DIR "${OOX_BINARY_DIR}/exception_policy_compile_build")
file(MAKE_DIRECTORY "${OOX_TRY_SRC_DIR}")
file(MAKE_DIRECTORY "${OOX_TRY_BUILD_DIR}")

file(WRITE "${OOX_TRY_SRC_DIR}/CMakeLists.txt" "
cmake_minimum_required(VERSION 3.12)
project(ExceptionPolicyCompile LANGUAGES CXX)
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_TRY_COMPILE_TARGET_TYPE STATIC_LIBRARY)

set(OOX_SOURCE_DIR \"${OOX_SOURCE_DIR}\")
set(CMAKE_CXX_FLAGS \"${CMAKE_CXX_FLAGS} -I${OOX_SOURCE_DIR}\")

file(WRITE \"${OOX_TRY_SRC_DIR}/ok.cpp\" \"
#include <oox/oox.h>
int main() {
#if defined(__cpp_exceptions)
    oox::var<int, false> base = oox::run<false>([]() { return 1; });
    oox::var<int, true> dep = oox::run<true>([](int x) { return x + 1; }, base);
    (void)dep;
#endif
    return 0;
}
\")

try_compile(OOX_OK_RESULT
            \"${OOX_TRY_BUILD_DIR}/ok_build\"
            \"${OOX_TRY_SRC_DIR}/ok.cpp\"
            CMAKE_FLAGS
              \"-DCMAKE_CXX_STANDARD=20\"
              \"-DCMAKE_CXX_STANDARD_REQUIRED=ON\"
              \"-DCMAKE_CXX_EXTENSIONS=OFF\"
            OUTPUT_VARIABLE OOX_OK_OUTPUT)
if (NOT OOX_OK_RESULT)
  message(FATAL_ERROR \"Expected ok.cpp to compile. Output:\\n${OOX_OK_OUTPUT}\")
endif()

file(WRITE \"${OOX_TRY_SRC_DIR}/fail.cpp\" \"
#include <oox/oox.h>
int main() {
    oox::var<int, true> base = oox::run<true>([]() { return 1; });
    oox::var<int, false> dep = oox::run<false>([](int x) { return x + 1; }, base);
    (void)dep;
    return 0;
}
\")

try_compile(OOX_FAIL_RESULT
            \"${OOX_TRY_BUILD_DIR}/fail_build\"
            \"${OOX_TRY_SRC_DIR}/fail.cpp\"
            CMAKE_FLAGS
              \"-DCMAKE_CXX_STANDARD=20\"
              \"-DCMAKE_CXX_STANDARD_REQUIRED=ON\"
              \"-DCMAKE_CXX_EXTENSIONS=OFF\"
            OUTPUT_VARIABLE OOX_FAIL_OUTPUT)
if (OOX_FAIL_RESULT)
  message(FATAL_ERROR \"Expected fail.cpp to fail to compile. Output:\\n${OOX_FAIL_OUTPUT}\")
endif()

file(WRITE \"${OOX_TRY_SRC_DIR}/fail_write.cpp\" \"
#include <oox/oox.h>
int main() {
    oox::var<int, false> out = oox::run<false>([]() { return 0; });
    oox::run<true>([](oox::var<int, false>& v) { v = 5; }, out);
    return 0;
}
\")

try_compile(OOX_FAIL_WRITE_RESULT
            \"${OOX_TRY_BUILD_DIR}/fail_write_build\"
            \"${OOX_TRY_SRC_DIR}/fail_write.cpp\"
            CMAKE_FLAGS
              \"-DCMAKE_CXX_STANDARD=20\"
              \"-DCMAKE_CXX_STANDARD_REQUIRED=ON\"
              \"-DCMAKE_CXX_EXTENSIONS=OFF\"
            OUTPUT_VARIABLE OOX_FAIL_WRITE_OUTPUT)
if (OOX_FAIL_WRITE_RESULT)
  message(FATAL_ERROR \"Expected fail_write.cpp to fail to compile. Output:\\n${OOX_FAIL_WRITE_OUTPUT}\")
endif()
")

execute_process(COMMAND ${CMAKE_COMMAND} -S "${OOX_TRY_SRC_DIR}" -B "${OOX_TRY_BUILD_DIR}"
                RESULT_VARIABLE OOX_TRY_RESULT)
if (NOT OOX_TRY_RESULT EQUAL 0)
  message(FATAL_ERROR "Exception policy compile checks failed")
endif()
