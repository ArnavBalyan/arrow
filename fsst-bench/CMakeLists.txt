cmake_minimum_required(VERSION 3.16)
project(fsst_bench CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(Arrow REQUIRED)
find_package(Parquet REQUIRED)

add_executable(fsst_bench fsst_bench.cc)
target_link_libraries(fsst_bench PRIVATE Parquet::parquet_shared Arrow::arrow_shared)
