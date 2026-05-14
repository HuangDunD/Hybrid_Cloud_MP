add_executable(remote_node server.cc)

target_link_libraries(remote_node
        global
        core
        storage
        ${BRPC_LIB}
        ${DYNAMIC_LIB}
        dl
        pthread
)
