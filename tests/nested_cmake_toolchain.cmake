function(oox_get_nested_cmake_args output)
  set(args -G "${OOX_GENERATOR}")
  if (OOX_GENERATOR_PLATFORM)
    list(APPEND args -A "${OOX_GENERATOR_PLATFORM}")
  endif ()
  if (OOX_GENERATOR_TOOLSET)
    list(APPEND args -T "${OOX_GENERATOR_TOOLSET}")
  endif ()
  foreach (setting
           CXX_COMPILER CXX_COMPILER_TARGET MAKE_PROGRAM TOOLCHAIN_FILE SYSROOT
           OSX_SYSROOT OSX_ARCHITECTURES OSX_DEPLOYMENT_TARGET BUILD_TYPE
           GENERATOR_INSTANCE)
    if (OOX_${setting})
      string(REPLACE ";" "\\;" escaped_value "${OOX_${setting}}")
      list(APPEND args -D "CMAKE_${setting}=${escaped_value}")
    endif ()
  endforeach ()
  set(${output} "${args}" PARENT_SCOPE)
endfunction()
