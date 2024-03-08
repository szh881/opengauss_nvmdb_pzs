file(GLOB TEST_SRC unittest/*.cpp)

add_executable(dbtest ${TEST_SRC} utest_main.cpp)

target_link_libraries(dbtest nvmdbcore gtest_main stdc++fs
        pactree tbb pmemobj pmem ${CMAKE_DL_LIBS})

add_test(NAME DBTest COMMAND dbtest)
