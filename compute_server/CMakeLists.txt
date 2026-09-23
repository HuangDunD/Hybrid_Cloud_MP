add_subdirectory(worker)

set(COMPUTE_STRATEGY_SOURCES
        strategies/eager.cc
        strategies/lazy.cc
        strategies/single_node.cc
        strategies/time_slice.cc
        strategies/two_phase_commit.cc
        strategies/two_phase_commit_fetch.cc
)

add_executable(compute_server
        run.cc
        server.cc
        ${COMPUTE_STRATEGY_SOURCES}
        ../core/recovery_interface/recovery_interface.cc
)

target_link_libraries(compute_server
        worker
        global
        core
        sql
        ${BRPC_LIB}
        ${DYNAMIC_LIB}
)
