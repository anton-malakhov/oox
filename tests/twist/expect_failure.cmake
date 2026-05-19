if (NOT DEFINED TEST_EXE)
  message(FATAL_ERROR "TEST_EXE is required")
endif ()

execute_process(
        COMMAND "${TEST_EXE}"
        RESULT_VARIABLE result
        OUTPUT_VARIABLE output
        ERROR_VARIABLE error
)

if (result EQUAL 0)
  message(FATAL_ERROR "Expected ${TEST_EXE} to fail, but it exited successfully")
endif ()

message(STATUS "Expected failure observed from ${TEST_EXE} with exit code ${result}")
if (error)
  message(STATUS "${error}")
endif ()
if (output)
  message(STATUS "${output}")
endif ()
