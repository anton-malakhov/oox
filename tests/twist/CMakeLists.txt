function(oox_add_twist_executable target source)
  add_executable(${target} ${source})

  target_link_libraries(${target}
          PRIVATE
          ${PROJECT_NAME}
          twist
  )
  target_compile_features(${target} PRIVATE cxx_std_23)

  target_compile_definitions(${target}
          PRIVATE
          HAVE_TWIST=1
          OOX_TWIST_TEST=1
          OOX_TWIST_MAX_STEPS=${OOX_TWIST_MAX_STEPS}
          OOX_TWIST_MAX_PREEMPTS=${OOX_TWIST_MAX_PREEMPTS}
          OOX_TWIST_RANDOM_SEEDS=${OOX_TWIST_RANDOM_SEEDS}
  )
endfunction()

function(oox_add_twist_test target source)
  oox_add_twist_executable(${target} ${source})

  add_test(NAME ${target} COMMAND ${target})
  set_tests_properties(${target} PROPERTIES
          LABELS "twist;concurrency"
          TIMEOUT 30
  )
endfunction()

oox_add_twist_test(test_twist_smoke test_twist_smoke.cpp)
oox_add_twist_test(test_twist_dependencies test_twist_dependencies.cpp)
oox_add_twist_test(test_twist_publication test_twist_publication.cpp)
oox_add_twist_test(test_twist_forwarding test_twist_forwarding.cpp)
oox_add_twist_test(test_twist_ordering test_twist_ordering.cpp)
oox_add_twist_test(test_twist_lifetime test_twist_lifetime.cpp)
oox_add_twist_test(test_twist_lifetime_trace_model test_twist_lifetime_trace_model.cpp)

if (TWIST_SIM)
  oox_add_twist_test(test_twist_known_deferred_race test_twist_known_deferred_race.cpp)
  set_tests_properties(test_twist_known_deferred_race PROPERTIES
          LABELS "twist;concurrency"
  )
endif ()
