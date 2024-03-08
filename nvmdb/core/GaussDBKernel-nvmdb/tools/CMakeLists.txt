add_executable(datahack datahack.cpp)
target_link_libraries(datahack nvmdbcore stdc++fs pactree tbb pmemobj pmem ${CMAKE_DL_LIBS})
