set(CMAKE_VERBOSE_MAKEFILE ON)
set(CMAKE_RULE_MESSAGES OFF)
set(CMAKE_SKIP_RPATH TRUE)

set(NVM_CORE_PATH ${CMAKE_CURRENT_SOURCE_DIR}/core)
set(NVM_FDW_ADAPTER_PATH ${CMAKE_CURRENT_SOURCE_DIR}/fdw_adapter)
set(NVM_MACRO_OPTIONS -DNVMDB_ADAPTER)

if ("$ENV{DEBUG_TYPE}" STREQUAL "release")
    set(NVM_MACRO_OPTIONS ${NVM_MACRO_OPTIONS} -DNDEBUG)
else()
    set(NVM_MACRO_OPTIONS ${NVM_MACRO_OPTIONS} -DDEBUG)
endif()

set(NVM_INCLUDE_PATH
    ${CMAKE_CURRENT_SOURCE_DIR}/core/GaussDBKernel-nvmdb/fdw_adapter
    ${CMAKE_CURRENT_SOURCE_DIR}/core/GaussDBKernel-nvmdb/include
    ${CMAKE_CURRENT_SOURCE_DIR}/core/GaussDBKernel-nvmdb/include/codec
    ${CMAKE_CURRENT_SOURCE_DIR}/core/GaussDBKernel-nvmdb/include/index
    ${CMAKE_CURRENT_SOURCE_DIR}/core/GaussDBKernel-nvmdb/include/undo 
    ${CMAKE_CURRENT_SOURCE_DIR}/core/GaussDBKernel-nvmdb/include/heap
    ${CMAKE_CURRENT_SOURCE_DIR}/core/GaussDBKernel-nvmdb/include/table_space
    ${CMAKE_CURRENT_SOURCE_DIR}/core/GaussDBKernel-nvmdb/third_parties/include
    ${CMAKE_CURRENT_SOURCE_DIR}/core/GaussDBKernel-nvmdb/third_parties/pactree/include
    ${CMAKE_CURRENT_SOURCE_DIR}/core/GaussDBKernel-nvmdb/third_parties/pactree/src
    ${CMAKE_CURRENT_SOURCE_DIR}/core/GaussDBKernel-nvmdb/third_parties/pactree/lib/PDL-ART
)

include_directories("${TBB_INCLUDE_PATH}")
include_directories("${PMDK_INCLUDE_PATH}")

set(CMAKE_MODULE_PATH
    ${NVM_CORE_PATH}
    ${NVM_FDW_ADAPTER_PATH}
)

add_subdirectory(core)
add_subdirectory(fdw_adapter)

