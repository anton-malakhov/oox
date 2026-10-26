cmake_minimum_required(VERSION 3.24)
project(oox_eigen_twist LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
if(NOT CMAKE_CXX_COMPILER_ID MATCHES "Clang" OR CMAKE_CXX_COMPILER_VERSION VERSION_LESS 21)
  message(FATAL_ERROR "This isolated Twist configuration requires LLVM Clang 21 or newer")
endif()

include(FetchContent)
set(TWIST_SIM ON CACHE BOOL "Deterministic scheduler simulation" FORCE)
set(TWIST_FAULTY ON CACHE BOOL "Explore synchronization failures" FORCE)
set(TWIST_SIM_ISOLATION OFF CACHE BOOL "Use portable non-isolated simulation" FORCE)
set(TWIST_SIM_DEBUG OFF CACHE BOOL "Avoid platform stacktrace dependencies" FORCE)
set(TWIST_ED_STD_ATOMIC_WAIT ON CACHE BOOL "Enable pool atomic wait support" FORCE)
set(TWIST_TESTS OFF CACHE BOOL "" FORCE)
set(TWIST_EXAMPLES OFF CACHE BOOL "" FORCE)
set(TWIST_DEVELOPER OFF CACHE BOOL "" FORCE)
set(SURE_TESTS OFF CACHE BOOL "" FORCE)
set(SURE_EXAMPLES OFF CACHE BOOL "" FORCE)
set(SURE_DEVELOPER OFF CACHE BOOL "" FORCE)

# First declarations win, overriding upstream's moving branch dependencies.
FetchContent_Declare(sure GIT_REPOSITORY https://gitlab.com/Lipovsky/sure.git
  GIT_TAG 0de37d2c7a97aa3764d58b319f89d066b72d9e22)
FetchContent_Declare(sure-stack GIT_REPOSITORY https://gitlab.com/Lipovsky/sure-stack.git
  GIT_TAG 510783c501d9162082fed7e20c7e6c579007bec0)
FetchContent_Declare(function2 GIT_REPOSITORY https://github.com/Naios/function2.git
  GIT_TAG 496c20fede5378190fff471fcd7ee3d8705e5096)
FetchContent_Declare(fmt GIT_REPOSITORY https://github.com/fmtlib/fmt.git
  GIT_TAG f87a75e15262614ce8aa0e6a0664df9d0823fb92)
FetchContent_Declare(backward GIT_REPOSITORY https://github.com/bombela/backward-cpp.git
  GIT_TAG 78b2541c9d3c44bc81e2c5f4f34aa3503ac985de)
FetchContent_Declare(json
  URL https://github.com/nlohmann/json/releases/download/v3.11.3/json.tar.xz
  URL_HASH SHA256=d6c65aca6b1ed68e7a182f4757257b107ae403032760ed6ef121c9d55e81757d)
FetchContent_Declare(twist GIT_REPOSITORY https://gitlab.com/Lipovsky/twist.git
  GIT_TAG 407b44e51a9d53c9fd5cbada08ada7a4a1cf029d)
FetchContent_MakeAvailable(twist)

get_filename_component(OOX_SOURCE_ROOT "${CMAKE_CURRENT_SOURCE_DIR}/../.." ABSOLUTE)
enable_testing()
foreach(component queues scheduler allocator ranges completion)
  add_executable(test_eigen_twist_${component} tests_${component}.cpp)
  target_include_directories(test_eigen_twist_${component} PRIVATE "${OOX_SOURCE_ROOT}")
  target_compile_definitions(test_eigen_twist_${component} PRIVATE OOX_EIGEN_TWIST=1)
  target_link_libraries(test_eigen_twist_${component} PRIVATE twist)
  add_test(NAME eigen_twist_${component} COMMAND test_eigen_twist_${component})
  set_tests_properties(eigen_twist_${component} PROPERTIES TIMEOUT 180 LABELS "eigen;twist")
endforeach()
