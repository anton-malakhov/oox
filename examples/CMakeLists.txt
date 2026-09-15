# Copyright (C) 2026
#
# SPDX-License-Identifier: Apache-2.0

add_executable(accounts_example accounts.cpp)
target_link_libraries(accounts_example PRIVATE ${PROJECT_NAME})
target_compile_features(accounts_example PRIVATE cxx_std_20)
