cmake_minimum_required(VERSION 3.28)

project(nplayer
    VERSION 0.1.0
    LANGUAGES CXX
)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release)
endif()

# Header-only git submodules. Their tests and examples stay off because they
# are not the top-level project.
add_subdirectory(third_party/nlib)
add_subdirectory(third_party/CLI11)

find_package(Arrow REQUIRED)
find_package(Parquet REQUIRED)
find_package(fmt REQUIRED)
find_package(PkgConfig REQUIRED)
pkg_check_modules(zstd REQUIRED IMPORTED_TARGET libzstd)

add_executable(nplayer
    src/main.cpp
    src/Parser.cpp
    src/ParquetWriter.cpp
)
target_include_directories(nplayer PRIVATE include)
target_link_libraries(nplayer PRIVATE
    nlib::nlib
    CLI11::CLI11
    Arrow::arrow_shared
    Parquet::parquet_shared
    fmt::fmt
    PkgConfig::zstd
)
# nlib sizes cache-line padding with std::hardware_destructive_interference_size; GCC warns
# that the value may change between builds, which is harmless for a non-ABI type.
target_compile_options(nplayer PRIVATE -Wall -Wextra -Wpedantic -Wno-interference-size)
