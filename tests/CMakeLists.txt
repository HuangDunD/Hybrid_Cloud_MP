add_executable(concurrency_test
    test_cases/concurrency_test/concurrency_test_main.cc
    test_cases/concurrency_test/concurrency_test.cc
)

target_link_libraries(concurrency_test
    pthread
    readline
)

add_executable(cache_consistency_test
    test_cases/cache_consistency_test/cache_consistency_test_main.cc
    test_cases/cache_consistency_test/cache_consistency_test.cc
)

target_link_libraries(cache_consistency_test
    pthread
    readline
)

# 元组锁等待 / 死锁检测（WAIT_DETECT）单元测试：组件级 + 进程内端到端
add_executable(lock_wait_test
    test_cases/lock_wait_test/lock_wait_test_main.cc
)

target_link_libraries(lock_wait_test
    global
    core
    ${BRPC_LIB}
    ${DYNAMIC_LIB}
    z
)

find_program(GOLD_LINKER NAMES ld.gold)
if(GOLD_LINKER)
    target_link_options(lock_wait_test PRIVATE "LINKER:-fuse-ld=gold")
endif()

add_executable(fsm_layout_test test_cases/fsm_layout_test.cc)
target_link_libraries(fsm_layout_test storage load_table core global ${BRPC_LIB} ${DYNAMIC_LIB})

add_executable(prepared_2pc_test test_cases/prepared_2pc_test.cc)
target_link_libraries(prepared_2pc_test storage core global ${BRPC_LIB} ${DYNAMIC_LIB})

add_executable(prepare_notification_test test_cases/prepare_notification_test.cc)
target_link_libraries(prepare_notification_test
    smallbank_db tpcc_db ycsb_db storage core global ${BRPC_LIB} ${DYNAMIC_LIB})

add_executable(page_cost_model_test test_cases/page_cost_model_test.cc)
target_link_libraries(page_cost_model_test core global ${BRPC_LIB} ${DYNAMIC_LIB})

add_executable(online_commit_cost_test test_cases/online_commit_cost_test.cc)
target_link_libraries(online_commit_cost_test core global ${BRPC_LIB} ${DYNAMIC_LIB})

add_executable(remote_tso_test test_cases/remote_tso_test.cc)
target_link_libraries(remote_tso_test core ${BRPC_LIB} ${DYNAMIC_LIB})

add_executable(mvcc_protocol_test test_cases/mvcc_protocol_test.cc)
target_link_libraries(mvcc_protocol_test mvcc_protocol)
# Compile both the engine and tests with NDEBUG: required protocol CHECKs remain.
add_executable(mvcc_protocol_ndebug_test test_cases/mvcc_protocol_test.cc ../core/mvcc/protocol.cc)
target_compile_definitions(mvcc_protocol_ndebug_test PRIVATE NDEBUG)
target_link_libraries(mvcc_protocol_ndebug_test ${BRPC_LIB} ${DYNAMIC_LIB})
add_executable(mvcc_publication_rpc_test test_cases/mvcc_publication_rpc_test.cc)
target_link_libraries(mvcc_publication_rpc_test mvcc_publication)

add_executable(undo_storage_test test_cases/undo_storage_test.cc)
target_link_libraries(undo_storage_test storage load_table core global ${BRPC_LIB} ${DYNAMIC_LIB})

# Recompile P2 implementation as well as its checks with NDEBUG.
add_executable(undo_storage_ndebug_test test_cases/undo_storage_test.cc
    ../core/undo/format.cc ../core/undo/redo.cc ../core/undo/store.cc
    ../core/undo/manager.cc)
target_compile_definitions(undo_storage_ndebug_test PRIVATE NDEBUG)
target_link_libraries(undo_storage_ndebug_test storage load_table core global ${BRPC_LIB} ${DYNAMIC_LIB})

add_executable(lazy_mvcc_test test_cases/lazy_mvcc_test.cc
    ../compute_server/server.cc ../compute_server/baseline_server.cc
    ../compute_server/lazyrelease_server.cc ../compute_server/twopc_server.cc
    ../compute_server/twopc_fetch_server.cc ../compute_server/single_server.cc
    ../core/sql_executor/scan.cc)
target_link_libraries(lazy_mvcc_test worker global core sql ${BRPC_LIB} ${DYNAMIC_LIB})

add_executable(lazy_mvcc_cluster_node test_cases/lazy_mvcc_cluster_node.cc
    ../compute_server/server.cc ../compute_server/baseline_server.cc
    ../compute_server/lazyrelease_server.cc ../compute_server/twopc_server.cc
    ../compute_server/twopc_fetch_server.cc ../compute_server/single_server.cc
    ../core/sql_executor/scan.cc)
target_compile_definitions(lazy_mvcc_cluster_node PRIVATE MVCC_CLUSTER_TEST)
target_link_libraries(lazy_mvcc_cluster_node worker global core sql ${BRPC_LIB} ${DYNAMIC_LIB})

add_executable(mvcc_cluster_test test_cases/mvcc_cluster_test.cc)
target_link_libraries(mvcc_cluster_test lazy_mvcc mvcc_publication ${BRPC_LIB} ${DYNAMIC_LIB})

add_executable(sql_mvcc_replay_check test_cases/sql_mvcc_replay_check.cc)
target_link_libraries(sql_mvcc_replay_check lazy_mvcc mvcc_publication ${BRPC_LIB} ${DYNAMIC_LIB})
