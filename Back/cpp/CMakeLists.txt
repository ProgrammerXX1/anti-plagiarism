# cpp/CMakeList.txt
cmake_minimum_required(VERSION 3.15)
project(plagio_native CXX)

set(CMAKE_CXX_STANDARD 17)

add_library(searchcore SHARED search_core.cpp)
target_include_directories(searchcore PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

add_executable(index_builder index_builder.cpp)
target_link_libraries(index_builder PRIVATE stdc++)