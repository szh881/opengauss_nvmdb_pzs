file (GLOB pactree_SRCS "*.h" "*.cpp")
file (GLOB art_SRCS "../lib/PDL-ART/*.h" "*.cpp")

add_library(pactree STATIC
    ${pactree_SRCS} ${art_SRCS}
)

target_include_directories(pactree PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}
)
INSTALL(TARGETS pactree
        ARCHIVE DESTINATION ${CMAKE_SOURCE_DIR}
        )
