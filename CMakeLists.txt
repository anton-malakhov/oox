# Copyright (C) 2021 Intel Corporation
#
# SPDX-License-Identifier: Apache-2.0

cmake_minimum_required (VERSION 3.12)
MESSAGE(STATUS "CMAKE_ROOT=" ${CMAKE_ROOT})

# Project name
project(OOX VERSION 0.2.0 LANGUAGES CXX)
set (CMAKE_CXX_STANDARD 20)

include(FetchContent)

FetchContent_Declare(
        googletest
        GIT_REPOSITORY https://github.com/google/googletest.git
        GIT_TAG v1.17.0
)
FetchContent_MakeAvailable(googletest)

# build options
option(OOX_BUILD_BENCHMARKS "Enables builds of benchmarks" ON)
option(OOX_BUILD_TESTS "Enables builds of tests" ON)
option(OOX_ENABLE_TBB "Enable TBB for threading (priority)" ON)
option(OOX_LOCAL_TBB "Force local copy of TBB" OFF)
option(OOX_ENABLE_TF "Enable Taskflow for threading" ON)
option(OOX_LOCAL_TF "Force local copy of TaskFlow" OFF)
option(OOX_ENABLE_FOLLY "Enable Folly for threading" ON)
option(OOX_LOCAL_FOLLY "Force local copy of Folly" OFF)
option(OOX_ENABLE_OMP "Enable OpenMP for comparison (if found)" ON)

# Global features
if(OOX_SANITIZE)
  add_compile_options(-fsanitize=${OOX_SANITIZE} -g -O1)
  add_link_options(-fsanitize=${OOX_SANITIZE} -g -O1)
  MESSAGE(STATUS "Building with -fsanitize=" ${OOX_SANITIZE})
  set(TBB_SANITIZE ${OOX_SANITIZE})
endif()

# Turn on the verbose
set(CMAKE_VERBOSE_MAKEFILE ON)

# Include additional language check
include(CheckLanguage)

# Adhere to GNU conventions
include(GNUInstallDirs)

# Enable test
include(CTest)

# Find packages
find_package(Threads REQUIRED)

if(OOX_ENABLE_TBB)
  if(NOT OOX_LOCAL_TBB)
    find_package(TBB 2021 QUIET)  # not REQUIRED
  endif()
  if(NOT TBB_FOUND)
    execute_process(COMMAND git submodule update --init --depth=1 thirdparty/oneTBB/
                    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR} )
    option(TBB_TEST OFF)
    add_subdirectory(thirdparty/oneTBB onetbb)
    set(TBB_FOUND ON)
  endif()
endif()

if(OOX_ENABLE_TF)
  if(NOT OOX_LOCAL_TF)
    find_package(Taskflow QUIET)  # not REQUIRED
  endif()
  if(NOT Taskflow_FOUND)
    execute_process(COMMAND git submodule update --init --depth=1 thirdparty/taskflow/
                    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR} )
    option(TF_BUILD_TESTS OFF)
    option(TF_BUILD_EXAMPLES OFF)
    add_subdirectory(thirdparty/taskflow taskflow)
    add_library(Taskflow::Taskflow ALIAS Taskflow)
    set(Taskflow_FOUND ON)
  endif()
endif()

if(OOX_ENABLE_OMP)
  find_package(OpenMP)
endif()

if(OOX_ENABLE_FOLLY)
  add_definitions(-DGLOG_USE_GLOG_EXPORT)
  find_package(Gflags CONFIG REQUIRED)

  # --- Make gflags_shared workable for Debug builds with CMake 4 + Ninja ---
  # Some gflags packages only export a single imported configuration (e.g.
  # Release or "NOCONFIG"). For a Debug build, CMake 4.0 will complain if
  # gflags_shared(Debug) has no IMPORTED_LOCATION_*. We copy any existing
  # location to IMPORTED_LOCATION_DEBUG and *avoid* MAP_IMPORTED_CONFIG_DEBUG.
  if(TARGET gflags_shared)
    get_target_property(_gflags_imported gflags_shared IMPORTED)
    if(_gflags_imported)
      set(_gflags_src_loc "")

      # Try to reuse an existing config-specific location if present.
      foreach(cfg Release RelWithDebInfo MinSizeRel)
        get_target_property(_loc gflags_shared IMPORTED_LOCATION_${cfg})
        if(_loc)
          set(_gflags_src_loc "${_loc}")
          break()
        endif()
      endforeach()

      # Fall back to config-less IMPORTED_LOCATION (NOCONFIG) if that's how
      # the package was exported.
      if(NOT _gflags_src_loc)
        get_target_property(_loc gflags_shared IMPORTED_LOCATION)
        if(_loc)
          set(_gflags_src_loc "${_loc}")
        endif()
      endif()

      if(_gflags_src_loc)
        get_target_property(_dbg_loc gflags_shared IMPORTED_LOCATION_DEBUG)
        if(NOT _dbg_loc)
          set_target_properties(gflags_shared PROPERTIES
                  IMPORTED_LOCATION_DEBUG "${_gflags_src_loc}"
          )
        endif()
      endif()
    endif()
  endif()

  option(FOLLY_USE_STATIC "Use static Folly" ON)

  if(NOT OOX_LOCAL_FOLLY)
    find_package(folly QUIET)  # not REQUIRED
  endif()
  if(folly_FOUND)
    add_library(folly INTERFACE)
    target_link_libraries(folly INTERFACE Folly::folly Folly::folly_deps)
  else()
    execute_process(
            COMMAND git submodule update --init --depth=1 thirdparty/folly/ thirdparty/fastfloat/
            WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    )
    set(FASTFLOAT_INCLUDE_DIR thirdparty/fastfloat/include)
    include_directories(thirdparty/fastfloat/include)

    # Replace install() to avoid fastfloat directory problem
    macro(install)
    endmacro()

    add_subdirectory(thirdparty/folly folly)
    set(folly_FOUND ON)
  endif()
endif()

# Library
add_library(${PROJECT_NAME} INTERFACE)
add_library(${PROJECT_NAME}::${PROJECT_NAME} ALIAS ${PROJECT_NAME})
target_compile_features(${PROJECT_NAME} INTERFACE cxx_std_11)
target_link_libraries(${PROJECT_NAME} INTERFACE Threads::Threads)
target_include_directories(${PROJECT_NAME} INTERFACE
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
  $<INSTALL_INTERFACE:include/> 
)

enable_testing()
if(OOX_BUILD_TESTS)
  add_subdirectory(tests)
endif(OOX_BUILD_TESTS)

if(OOX_BUILD_BENCHMARKS)
  add_subdirectory(benchmarks)
endif(OOX_BUILD_BENCHMARKS)

# -----------------------------------------------------------------------------
# create find_package(OOX CONFIG)
# -----------------------------------------------------------------------------

# install header
install(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/oox DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})

# export target
set_target_properties(${PROJECT_NAME} PROPERTIES EXPORT_NAME ${PROJECT_NAME})

export(
  TARGETS ${PROJECT_NAME} 
  NAMESPACE ${PROJECT_NAME}:: 
  FILE ${PROJECT_NAME}Targets.cmake
)
export(PACKAGE ${PROJECT_NAME})

install(TARGETS ${PROJECT_NAME} EXPORT ${PROJECT_NAME}Targets)
install(
  EXPORT ${PROJECT_NAME}Targets 
  NAMESPACE ${PROJECT_NAME}:: 
  DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/${PROJECT_NAME}
)

# set up config
include(CMakePackageConfigHelpers)

configure_package_config_file(
  ${PROJECT_NAME}Config.cmake.in
  ${CMAKE_CURRENT_BINARY_DIR}/${PROJECT_NAME}Config.cmake
  INSTALL_DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/${PROJECT_NAME}
  PATH_VARS CMAKE_INSTALL_FULL_INCLUDEDIR
)

write_basic_package_version_file(
  ${PROJECT_NAME}ConfigVersion.cmake 
  COMPATIBILITY SameMajorVersion
)

install(
  FILES ${CMAKE_CURRENT_BINARY_DIR}/${PROJECT_NAME}Config.cmake
        ${CMAKE_CURRENT_BINARY_DIR}/${PROJECT_NAME}ConfigVersion.cmake
  DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/${PROJECT_NAME}
)
