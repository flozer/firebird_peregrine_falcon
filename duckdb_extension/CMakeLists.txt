# duckdb_extension/CMakeLists.txt

cmake_minimum_required(VERSION 3.10)

# Define the duckdb_extension library
project(duckdb_extension)

# Add the library target
add_library(duckdb_extension SHARED
    src/firebird_extension.cpp
    src/firebird_scanner.cpp
)

# Include the parent directory's include folder
target_include_directories(duckdb_extension PRIVATE ../include)

# Use pkg-config to find and link against libfbclient
find_package(PkgConfig REQUIRED)
pkg_check_modules(FBCLIENT REQUIRED libfbclient)

if (FBCLIENT_FOUND)
    target_include_directories(duckdb_extension PRIVATE ${FBCLIENT_INCLUDE_DIRS})
    target_link_libraries(duckdb_extension ${FBCLIENT_LIBRARIES})
else()
    message(FATAL_ERROR "Could not find the Firebird client library (libfbclient) using pkg-config.")
endif()

# Set C++ standard
set_target_properties(duckdb_extension PROPERTIES
    CXX_STANDARD 17
    CXX_STANDARD_REQUIRED YES
)

# Ensure shared build
set_target_properties(duckdb_extension PROPERTIES
    POSITION_INDEPENDENT_CODE ON
)