find_package(Git REQUIRED)
set(patches fastlanes-build.patch fastlanes-branch-consolidation.patch fastlanes-ci.patch
            fastlanes-native-reader.patch fastlanes-portability.patch)
# Archive sources live inside the extension's worktree. Do not let git discover
# that parent repository: git apply would then silently skip these paths.
get_filename_component(source_parent "${FLS_SOURCE_DIR}" DIRECTORY)
set(git_apply "${CMAKE_COMMAND}" -E env "GIT_CEILING_DIRECTORIES=${source_parent}"
              "${GIT_EXECUTABLE}" apply)

# Later patches touch earlier patches' hunks. Unwind applicable layers in reverse
# order before reapplying the stack, so FetchContent reconfiguration is idempotent.
set(reverse_patches ${patches})
list(REVERSE reverse_patches)
foreach(patch_name IN LISTS reverse_patches)
    set(patch "${CMAKE_CURRENT_LIST_DIR}/../patches/${patch_name}")
    execute_process(COMMAND ${git_apply} --reverse --check "${patch}"
                    WORKING_DIRECTORY "${FLS_SOURCE_DIR}"
                    RESULT_VARIABLE applied OUTPUT_QUIET ERROR_QUIET)
    if (applied EQUAL 0)
        execute_process(COMMAND ${git_apply} --reverse "${patch}"
                        WORKING_DIRECTORY "${FLS_SOURCE_DIR}"
                        RESULT_VARIABLE result ERROR_VARIABLE error)
        if (NOT result EQUAL 0)
            message(FATAL_ERROR "Could not reverse ${patch_name}: ${error}")
        endif ()
    endif ()
endforeach()

foreach(patch_name IN LISTS patches)
    execute_process(COMMAND ${git_apply} "${CMAKE_CURRENT_LIST_DIR}/../patches/${patch_name}"
                    WORKING_DIRECTORY "${FLS_SOURCE_DIR}"
                    RESULT_VARIABLE result ERROR_VARIABLE error)
    if (NOT result EQUAL 0)
        message(FATAL_ERROR "Could not apply ${patch_name}: ${error}")
    endif ()
endforeach()
