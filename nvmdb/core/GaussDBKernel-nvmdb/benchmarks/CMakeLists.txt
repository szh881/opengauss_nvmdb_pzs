add_executable(smallbank smallbank.cpp)
target_link_libraries(smallbank nvmdbcore pactree stdc++fs tbb pmemobj pmem ${CMAKE_DL_LIBS})

add_executable(index index.cpp)
target_link_libraries(index nvmdbcore pactree stdc++fs tbb pmemobj pmem ${CMAKE_DL_LIBS})

add_executable(numaheap numaheap.cpp)
target_link_libraries(numaheap nvmdbcore pactree stdc++fs tbb pmemobj pmem ${CMAKE_DL_LIBS})

add_executable(tpcc testtpcc.cpp)
target_link_libraries(tpcc nvmdbcore pactree stdc++fs tbb pmemobj pmem ${CMAKE_DL_LIBS} -ljemalloc)
