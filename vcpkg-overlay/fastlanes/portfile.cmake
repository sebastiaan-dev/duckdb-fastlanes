vcpkg_from_github(
        OUT_SOURCE_PATH SOURCE_PATH
        REPO sebastiaan-dev/FastLanes
        REF cmake-overrride
        SHA512 2aeea831660dc59af788c511f70308f5ef48ca34eb3ab4cfbf39019ad27312e31b5d348f28f3d4ed77a17f83bdb92ea9424e02fad88e53b1908ce9c46ee4f7b3
)

vcpkg_cmake_configure(
        SOURCE_PATH "${SOURCE_PATH}"
)

vcpkg_cmake_install()
vcpkg_copy_pdbs()

file(INSTALL
        FILES
        "${CURRENT_PACKAGES_DIR}/lib/FLS/FLSTargets.cmake"
        DESTINATION "${CURRENT_PACKAGES_DIR}/share/${PORT}"
        RENAME "fastlanesTargets.cmake"
)

file(INSTALL
        FILES
        "${CURRENT_PACKAGES_DIR}/debug/lib/FLS/FLSTargets-debug.cmake"
        DESTINATION "${CURRENT_PACKAGES_DIR}/debug/share/${PORT}"
        RENAME "fastlanesTargets-debug.cmake"
)

set(_cfg "${CURRENT_PACKAGES_DIR}/share/${PORT}/fastlanesConfig.cmake")
file(WRITE "${_cfg}"
        "@PACKAGE_INIT@\n\
include(\"\${CMAKE_CURRENT_LIST_DIR}/fastlanesTargets.cmake\")\n"
)

set(_debug_cfg "${CURRENT_PACKAGES_DIR}/debug/share/${PORT}/fastlanesConfig.cmake")
file(WRITE "${_debug_cfg}"
        "@PACKAGE_INIT@\n\
include(\"\${CMAKE_CURRENT_LIST_DIR}/fastlanesTargets-debug.cmake\")\n"
)