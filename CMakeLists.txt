cmake_minimum_required(VERSION 3.22)

option(FLS_NO_TRANSPOSE "Use fastlanes::copy instead of untranspose" ON)

if (FLS_NO_TRANSPOSE)
    add_compile_definitions(FLS_NO_TRANSPOSE=0)
endif ()

# Set extension name here
set(TARGET_NAME fastlanes)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})

set(CMAKE_CXX_STANDARD 20 CACHE STRING "C++ standard" FORCE)
set(CMAKE_CXX_STANDARD_REQUIRED ON CACHE BOOL "Require C++ standard" FORCE)
set(CMAKE_CXX_EXTENSIONS OFF CACHE BOOL "Disable extensions" FORCE)
# We use C++20, but no modules. Avoid generated .modmap response files in the
# compilation database: configure-only clang-tidy runs cannot read those files.
set(CMAKE_CXX_SCAN_FOR_MODULES OFF)
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
if (POLICY CMP0135)
    cmake_policy(SET CMP0135 NEW)
endif ()
option(FLS_EXTENSION_BUILD_EXAMPLES "Build Examples Option" OFF)
#### TODO: enable
option(FLS_ENABLE_CLANG_TIDY "Enable clang_tidy on all targets" OFF)

add_compile_definitions(FLS_EXTENSION_CMAKE_SOURCE_DIR="${CMAKE_CURRENT_SOURCE_DIR}")

# The extension relies on the writer, projection, table-API and positional-I/O
# patches in this revision, not vanilla cwida/FastLanes. Override locally with
# FETCHCONTENT_SOURCE_DIR_FLS_REPO (or FASTLANES_SOURCE_DIR via make).
# Include patch contents in the FetchContent command so an existing build
# reruns the patch step when this patch series changes.
set(FLS_PATCH_SIGNATURE "")
foreach(patch_file cmake/patch_fastlanes.cmake patches/fastlanes-build.patch
                   patches/fastlanes-branch-consolidation.patch patches/fastlanes-ci.patch)
    set_property(DIRECTORY APPEND PROPERTY CMAKE_CONFIGURE_DEPENDS "${CMAKE_CURRENT_SOURCE_DIR}/${patch_file}")
    file(SHA256 "${CMAKE_CURRENT_SOURCE_DIR}/${patch_file}" patch_hash)
    string(APPEND FLS_PATCH_SIGNATURE "${patch_hash}")
endforeach()

FetchContent_Declare(
        fls_repo
        URL https://codeload.github.com/sebastiaan-dev/fastlanes-patched/tar.gz/7f91afdbd916d24ab5b39a990fbdf7d9157c0f69
        URL_HASH SHA256=5cc986dab44d4a2d2b1fd8fd2e32645cd6cc1e9e03e950b1cb1615ee5415d5de
        PATCH_COMMAND ${CMAKE_COMMAND} -DFLS_SOURCE_DIR=<SOURCE_DIR>
                      -DFLS_PATCH_SIGNATURE=${FLS_PATCH_SIGNATURE}
                      -P ${CMAKE_CURRENT_SOURCE_DIR}/cmake/patch_fastlanes.cmake
)

FetchContent_MakeAvailable(fls_repo)
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
