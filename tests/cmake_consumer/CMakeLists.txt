cmake_minimum_required(VERSION 3.14)
project(OOXConsumer LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
find_package(OOX CONFIG REQUIRED)

add_executable(oox_consumer main.cpp)
target_link_libraries(oox_consumer PRIVATE OOX::OOX)
