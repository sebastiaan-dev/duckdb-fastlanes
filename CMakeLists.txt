cmake_minimum_required(VERSION 3.5)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_POSITION_INDEPENDENT_CODE TRUE)

# Set extension name here
set(TARGET_NAME fast_lanes)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})
include_directories(src/include)

set(EXTENSION_SOURCES src/fast_lanes_extension.cpp
        src/read_fast_lanes.cpp
        src/reader/fast_lanes_multi_file_info.cpp
        src/reader/fast_lanes_reader.cpp
        src/reader/translation_utils.cpp
        src/reader/materializer.cpp
        src/write_fast_lanes.cpp
        src/writer/translation_utils.cpp
)

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

##-----------------------------------------------------------------------------------------------------------------------
## FLS :
include(FetchContent)
option(FLS_EXTENSION_BUILD_EXAMPLES "Build Examples Option" OFF)
#### TODO: enable
option(FLS_ENABLE_CLANG_TIDY "Enable clang_tidy on all targets" OFF)

#### Definitions: ---------------------------------------------------------------------------------------------------------
add_compile_definitions(FLS_EXTENSION_CMAKE_SOURCE_DIR="${CMAKE_CURRENT_SOURCE_DIR}")
### FastLanes: -----------------------------------------------------------------------------------------------------------
# -- Use a remote instance of FastLanes -- #
#FetchContent_Declare(
#        fls_repo
#        GIT_REPOSITORY https://github.com/sebastiaan-dev/FastLanes.git
#        GIT_TAG dev
#)
#
#FetchContent_MakeAvailable(fls_repo)
#include_directories(${fls_repo_SOURCE_DIR}/include)
#
#target_link_libraries(${EXTENSION_NAME} FastLanes)
#target_link_libraries(${LOADABLE_EXTENSION_NAME} FastLanes)

# -- Use a local instance of FastLanes -- #
set(FASTLANES_DIR "/Users/sebastiaan/repos/sebastiaan-fastlanes")
add_subdirectory(${FASTLANES_DIR} ${CMAKE_CURRENT_BINARY_DIR}/FastLanes_build)

include_directories(${FASTLANES_DIR}/include)

target_link_libraries(${EXTENSION_NAME} FastLanes)
target_link_libraries(${LOADABLE_EXTENSION_NAME} FastLanes)

#---------------------------------------------------

#add_subdirectory(benchmarks)

install(
        TARGETS ${EXTENSION_NAME}
        EXPORT "${DUCKDB_EXPORT_SET}"
        LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
        ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
