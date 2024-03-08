set(CMAKE_VERBOSE_MAKEFILE ON)
set(CMAKE_RULE_MESSAGES OFF)
set(CMAKE_SKIP_RPATH TRUE)

execute_process(COMMAND cat /proc/cpuinfo COMMAND grep clwb OUTPUT_VARIABLE CLWB)
if(CLWB)
    message(STATUS "Use clwb")
    set(nvm_core_DEF_USE_CLWB)
else()
    message(STATUS "Use clflush instead of clwb")
    set(nvm_core_DEF_USE_CLWB -DNO_CLWB)
endif()

set(nvm_core_DEF_OPTIONS ${MACRO_OPTIONS} ${nvm_core_DEF_USE_CLWB} ${NVM_MACRO_OPTIONS})

set(nvm_core_COMPILE_OPTIONS ${OPTIMIZE_OPTIONS} ${OS_OPTIONS} ${PROTECT_OPTIONS} ${BIN_SECURE_OPTIONS} ${CHECK_OPTIONS})

set(nvm_core_LINK_OPTIONS ${BIN_LINK_OPTIONS})

file(GLOB nvm_core_SRC 
    "GaussDBKernel-nvmdb/adapter/*.cpp"
    "GaussDBKernel-nvmdb/dbcore/*.cpp" 
    "GaussDBKernel-nvmdb/dbcore/undo/*.cpp" 
    "GaussDBKernel-nvmdb/dbcore/heap/*.cpp" 
    "GaussDBKernel-nvmdb/dbcore/table_space/*.cpp" 
    "GaussDBKernel-nvmdb/dbcore/codec/*.cpp" 
    "GaussDBKernel-nvmdb/dbcore/index/*.cpp"
    "GaussDBKernel-nvmdb/third_parties/pactree/src/*.cpp"
    "GaussDBKernel-nvmdb/third_parties/pactree/lib/PDL-ART/Tree.cpp"
    )

list(FILTER nvm_core_SRC EXCLUDE REGEX ".*GaussDBKernel-nvmdb/third_parties/pactree/src/main.cpp")

set(nvm_core_INC
    ${NVM_INCLUDE_PATH}
)

add_static_objtarget(gausskernel_storage_nvm_core nvm_core_SRC nvm_core_INC
    "${nvm_core_DEF_OPTIONS}" "${nvm_core_COMPILE_OPTIONS}" "${nvm_core_LINK_OPTIONS}")

