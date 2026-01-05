cmake_minimum_required(VERSION 3.11)
project(bench)
set(CMAKE_CXX_STANDARD 20)

set(CFS_ROOT_DIR "${PROJECT_SOURCE_DIR}/../cfs")

include_directories("${CFS_ROOT_DIR}/include")
include_directories("${CFS_ROOT_DIR}/lib/tbb/include")

link_directories("${CFS_ROOT_DIR}/build")
link_directories("${CFS_ROOT_DIR}/lib/tbb/build/tbb_build_release")

include(FetchContent)
FetchContent_Declare(json URL https://github.com/nlohmann/json/releases/download/v3.11.2/json.tar.xz)
FetchContent_Declare(spdlog GIT_REPOSITORY https://github.com/gabime/spdlog.git GIT_TAG v1.x)
FetchContent_Declare(googletest URL https://github.com/google/googletest/archive/refs/tags/release-1.12.1.zip)
FetchContent_Declare(cxxopts GIT_REPOSITORY https://github.com/jarro2783/cxxopts.git GIT_TAG v3.0.0)
FetchContent_MakeAvailable(json spdlog googletest cxxopts)
set_target_properties(spdlog PROPERTIES POSITION_INDEPENDENT_CODE ON)

add_subdirectory(leveldb)
target_compile_definitions(leveldb PRIVATE JL_LIBCFS)

include_directories("${PROJECT_SOURCE_DIR}")

set(COMMON_DEPS nlohmann_json::nlohmann_json spdlog::spdlog)
set(CFS_DEPS libcfs.so libtbb.so.2 libtbbmalloc.so.2 pthread)

foreach (target bench prep)
    add_executable(${target} src/main_${target}.cpp src/args.cpp src/spec.cpp)
    target_link_libraries(${target} PRIVATE ${COMMON_DEPS} ${CFS_DEPS} cxxopts::cxxopts leveldb)
    if (CMAKE_BUILD_TYPE MATCHES Debug)
        target_compile_definitions(${target} PRIVATE SPDLOG_ACTIVE_LEVEL=SPDLOG_LEVEL_DEBUG)
    endif ()
endforeach ()

add_executable(tree src/main_tree.cpp)
target_link_libraries(tree PRIVATE ${COMMON_DEPS} ${CFS_DEPS})

add_executable(migration_test src/migration_test.cpp)
target_link_libraries(migration_test PRIVATE ${COMMON_DEPS} ${CFS_DEPS} leveldb)

add_executable(tests src/offset_test.cpp src/spec.cpp)
target_link_libraries(tests PRIVATE ${COMMON_DEPS} GTest::gtest_main)

# add memory sanitizer for all targets in Debug mode
if (CMAKE_BUILD_TYPE MATCHES Debug)
    get_property(targets DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR} PROPERTY BUILDSYSTEM_TARGETS)
    foreach (target ${targets})
        target_compile_options(${target} PRIVATE -fsanitize=address -fno-omit-frame-pointer)
        target_link_options(${target} PRIVATE -fsanitize=address)
        message(STATUS "Memory sanitizer enabled for ${target}")
    endforeach ()
endif ()
