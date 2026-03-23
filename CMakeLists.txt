cmake_minimum_required(VERSION 3.22)

option(FLS_NO_TRANSPOSE "Use fastlanes::copy instead of untranspose" ON)

if(FLS_NO_TRANSPOSE)
    add_compile_definitions(FLS_NO_TRANSPOSE=0)
endif()

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
        src/reader/row_group_statistics.cpp
        src/reader/row_group_filter.cpp
        src/reader/filter_executor.cpp
        src/reader/schema_builder.cpp
        src/reader/table_metadata.cpp
        src/reader/translation_utils.cpp
        src/reader/fls_decode.cpp

        src/write_fls.cpp
        src/writer/fls_view_writer.cpp
        src/writer/fls_writer.cpp
        src/writer/translation_utils.cpp
)

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

message(STATUS "C:   ${CMAKE_C_COMPILER} (ID=${CMAKE_C_COMPILER_ID})")
message(STATUS "C++: ${CMAKE_CXX_COMPILER} (ID=${CMAKE_CXX_COMPILER_ID})")

#-----------------------------------------------------------------------------------------------------------------------
include(FetchContent)
option(FLS_EXTENSION_BUILD_EXAMPLES "Build Examples Option" OFF)
#### TODO: enable
option(FLS_ENABLE_CLANG_TIDY "Enable clang_tidy on all targets" OFF)

add_compile_definitions(FLS_EXTENSION_CMAKE_SOURCE_DIR="${CMAKE_CURRENT_SOURCE_DIR}")

set(FASTLANES_DIR "/Users/sebastiaan/repos/fls-patch-projection")
add_subdirectory(${FASTLANES_DIR} ${CMAKE_CURRENT_BINARY_DIR}/FastLanes_build)
#FetchContent_Declare(
#        fls_repo
#        GIT_REPOSITORY https://github.com/cwida/FastLanes
#        GIT_TAG dev
#)

#FetchContent_MakeAvailable(fls_repo)
include_directories(${fls_repo_SOURCE_DIR}/include)


target_link_libraries(${EXTENSION_NAME} FastLanes)
target_link_libraries(${LOADABLE_EXTENSION_NAME} FastLanes)
#-----------------------------------------------------------------------------------------------------------------------

# add_subdirectory(scripts/data_generator/fls)

install(
        TARGETS ${EXTENSION_NAME}
        EXPORT "${DUCKDB_EXPORT_SET}"
        LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
        ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
