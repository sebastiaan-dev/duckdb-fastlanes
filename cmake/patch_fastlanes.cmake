find_package(Git REQUIRED)
set(patches fastlanes-build.patch fastlanes-branch-consolidation.patch)
# Archive sources live inside the extension's worktree. Do not let git discover
# that parent repository: git apply would then silently skip these paths.
get_filename_component(source_parent "${FLS_SOURCE_DIR}" DIRECTORY)
set(git_apply "${CMAKE_COMMAND}" -E env "GIT_CEILING_DIRECTORIES=${source_parent}"
              "${GIT_EXECUTABLE}" apply)

# FetchContent can rerun its patch step when reconfiguring. Accept an already
# applied patch, but fail rather than silently building an incompatible source.
foreach(patch_name IN LISTS patches)
    set(patch "${CMAKE_CURRENT_LIST_DIR}/../patches/${patch_name}")
    execute_process(COMMAND ${git_apply} --reverse --check "${patch}"
                    WORKING_DIRECTORY "${FLS_SOURCE_DIR}"
                    RESULT_VARIABLE already_applied OUTPUT_QUIET ERROR_QUIET)
    if (NOT already_applied EQUAL 0)
        execute_process(COMMAND ${git_apply} "${patch}"
                        WORKING_DIRECTORY "${FLS_SOURCE_DIR}"
                        RESULT_VARIABLE result ERROR_VARIABLE error)
        if (NOT result EQUAL 0)
            message(FATAL_ERROR "Could not apply ${patch_name}: ${error}")
        endif ()
    endif ()
endforeach()
