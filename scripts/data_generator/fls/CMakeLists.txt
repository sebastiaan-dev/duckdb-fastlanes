cmake_minimum_required(VERSION 3.10)

add_executable(generate_fls generate_fls.cpp)

target_link_libraries(generate_fls PRIVATE FastLanes)

install(TARGETS generate_fls
        RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR})
