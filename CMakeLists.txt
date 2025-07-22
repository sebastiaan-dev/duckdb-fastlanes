cmake_minimum_required(VERSION 3.22)

# Set extension name here
set(TARGET_NAME fastlanes)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})

set(CMAKE_CXX_STANDARD 20 CACHE STRING "C++ standard" FORCE)
set(CMAKE_CXX_STANDARD_REQUIRED ON CACHE BOOL "Require C++ standard" FORCE)
set(CMAKE_CXX_EXTENSIONS OFF CACHE BOOL "Disable extensions" FORCE)
set(CMAKE_POSITION_INDEPENDENT_CODE TRUE CACHE BOOL "Position Independent Code" FORCE)

include_directories(src/include)

set(EXTENSION_SOURCES src/fastlanes_extension.cpp
        src/read_fls.cpp
        src/reader/fls_multi_file_info.cpp
        src/reader/fls_reader.cpp
        src/reader/translation_utils.cpp
        src/write_fls.cpp
        src/writer/translation_utils.cpp
        src/writer/fls_view_writer.cpp
        src/writer/fls_writer.cpp
)

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

message(STATUS "C:   ${CMAKE_C_COMPILER} (ID=${CMAKE_C_COMPILER_ID})")
message(STATUS "C++: ${CMAKE_CXX_COMPILER} (ID=${CMAKE_CXX_COMPILER_ID})")

#-----------------------------------------------------------------------------------------------------------------------
#include(FetchContent)
#option(FLS_EXTENSION_BUILD_EXAMPLES "Build Examples Option" OFF)
##### TODO: enable
#option(FLS_ENABLE_CLANG_TIDY "Enable clang_tidy on all targets" OFF)
#
#add_compile_definitions(FLS_EXTENSION_CMAKE_SOURCE_DIR="${CMAKE_CURRENT_SOURCE_DIR}")
#
#FetchContent_Declare(
#        fls_repo
#        GIT_REPOSITORY https://github.com/sebastiaan-dev/FastLanes.git
#        GIT_TAG update
#)
#
#FetchContent_MakeAvailable(fls_repo)
#include_directories(${fls_repo_SOURCE_DIR}/include)
#
#
#target_link_libraries(${EXTENSION_NAME} FastLanes)
#target_link_libraries(${LOADABLE_EXTENSION_NAME} FastLanes)
#-----------------------------------------------------------------------------------------------------------------------
# -- Use a local instance of FastLanes -- #
set(FASTLANES_DIR "/Users/sebastiaan/repos/sebastiaan-fastlanes")
add_subdirectory(${FASTLANES_DIR} ${CMAKE_CURRENT_BINARY_DIR}/FastLanes_build)

include_directories(${FASTLANES_DIR}/include)

target_link_libraries(${EXTENSION_NAME} FastLanes)
target_link_libraries(${LOADABLE_EXTENSION_NAME} FastLanes)
#add_subdirectory(benchmarks)

install(
        TARGETS ${EXTENSION_NAME}
        EXPORT "${DUCKDB_EXPORT_SET}"
        LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
        ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
