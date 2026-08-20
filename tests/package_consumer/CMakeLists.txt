cmake_minimum_required(VERSION 3.14)
project(OOXEigenConsumer LANGUAGES CXX)

find_package(OOX CONFIG REQUIRED)

add_executable(oox_eigen_consumer main.cpp)
target_link_libraries(oox_eigen_consumer PRIVATE OOX::eigen)
