cmake_minimum_required(VERSION 3.5)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_POSITION_INDEPENDENT_CODE TRUE)

# Set extension name here
set(TARGET_NAME fast_lanes)

# DuckDB's extension distribution supports vcpkg. As such, dependencies can be added in ./vcpkg.json and then
# used in cmake with find_package. Feel free to remove or replace with other dependencies.
# Note that it should also be removed from vcpkg.json to prevent needlessly installing it..
#find_package(OpenSSL REQUIRED)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})
include_directories(src/include)

set(EXTENSION_SOURCES src/fast_lanes_extension.cpp src/read_fast_lanes.cpp)

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

# Link OpenSSL in both the static library as the loadable extension
#target_link_libraries(${EXTENSION_NAME} OpenSSL::SSL OpenSSL::Crypto)
#target_link_libraries(${LOADABLE_EXTENSION_NAME} OpenSSL::SSL OpenSSL::Crypto)

##-----------------------------------------------------------------------------------------------------------------------
## FLS :
#include(FetchContent)
#include(CMakePrintHelpers)
###
#option(FLS_EXTENSION_BUILD_EXAMPLES "Build Examples Option" OFF)
#### TODO: enable
#option(FLS_ENABLE_CLANG_TIDY "Enable clang_tidy on all targets" OFF)
###
###
#### Definitions: ---------------------------------------------------------------------------------------------------------
#add_compile_definitions(FLS_EXTENSION_CMAKE_SOURCE_DIR="${CMAKE_CURRENT_SOURCE_DIR}")
##
### FastLanes: -----------------------------------------------------------------------------------------------------------
#message("---------------------------------------------------------------------------------------------------------")
#message("-- FLS Extension: Build FastLanes ")
#
#FetchContent_Declare(
#        fls_repo
#        GIT_REPOSITORY https://github.com/cwida/FastLanes.git
#        GIT_TAG release_v0.1
#)
#
#FetchContent_MakeAvailable(fls_repo)
#include_directories(${fls_repo_SOURCE_DIR}/include)
#
#target_link_libraries(${EXTENSION_NAME} FastLanes)
#target_link_libraries(${LOADABLE_EXTENSION_NAME} FastLanes)

##-----------------------------------------------------------------------------------------------------------------------

#---------------------------------------------------
# Use a local instance of FastLanes
#
# Adjust the path below to where your local FastLanes source is located.
include(CMakePrintHelpers)
set(FASTLANES_DIR "/Users/sebastiaan/Documents/university/thesis/sebastiaan-fastlanes")
### TODO: enable
option(FLS_ENABLE_CLANG_TIDY "Enable clang_tidy on all targets" OFF)

add_compile_definitions(FLS_EXTENSION_CMAKE_SOURCE_DIR="${CMAKE_CURRENT_SOURCE_DIR}")

message("---------------------------------------------------------------------------------------------------------")
message("-- FLS Extension: Build FastLanes ")

# Add FastLanes as a subdirectory.
# This will build FastLanes as part of your project.
add_subdirectory(${FASTLANES_DIR} ${CMAKE_CURRENT_BINARY_DIR}/FastLanes_build)

# Include FastLanes headers.
include_directories(${FASTLANES_DIR}/include)

# Link your extension with the local FastLanes library target.
# (Make sure the target name "FastLanes" matches the one defined in the FastLanes CMakeLists.txt.)
target_link_libraries(${EXTENSION_NAME} FastLanes)
target_link_libraries(${LOADABLE_EXTENSION_NAME} FastLanes)

#---------------------------------------------------

add_subdirectory(benchmarks)

install(
        TARGETS ${EXTENSION_NAME}
        EXPORT "${DUCKDB_EXPORT_SET}"
        LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
        ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
