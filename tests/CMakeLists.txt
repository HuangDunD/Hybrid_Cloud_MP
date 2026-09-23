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

# FSM 并发单元测试：桩替换 ComputeServer 页面 RPC（模拟带 S/X 页面锁的页面服务器），
# 直接编译 fsm_tree.cc，不链接真实 RPC 实现
add_executable(fsm_test
    test_cases/fsm_test/fsm_test_main.cc
    ../core/fsm/fsm_tree.cc
)

target_link_libraries(fsm_test
    global
    ${BRPC_LIB}
    boost_coroutine
    boost_context
    boost_system
    ${PROTOBUF_LIBRARIES}
    ${LEVELDB_LIB}
    ${OPENSSL_CRYPTO_LIBRARY}
    ${OPENSSL_SSL_LIBRARY}
    ${GFLAGS_LIBRARY}
    pthread
)

add_test(NAME fsm_test COMMAND fsm_test)

# UndoArea 单元测试：独立 undo 区（docs/UNDO_DESIGN.md），
# 无 RPC 依赖，直接编译 undo_area.cc
add_executable(undo_area_test
    test_cases/undo_test/undo_area_test_main.cc
    ../core/storage/undo_area.cc
)

target_link_libraries(undo_area_test
    global
    ${BRPC_LIB}
    boost_coroutine
    boost_context
    boost_system
    ${PROTOBUF_LIBRARIES}
    ${LEVELDB_LIB}
    ${OPENSSL_CRYPTO_LIBRARY}
    ${OPENSSL_SSL_LIBRARY}
    ${GFLAGS_LIBRARY}
    dl
    pthread
)

add_test(NAME undo_area_test COMMAND undo_area_test)

add_executable(recovery_replay_wait_test test_cases/recovery_priority/replay_wait_test.cc)
add_executable(recovery_storage_test test_cases/recovery_priority/storage_recovery_test.cc)
add_executable(recovery_blink_path_test
    test_cases/recovery_priority/blink_path_test.cc
    ../core/index/bp_tree/blink/blink.cc)
target_compile_options(recovery_blink_path_test PRIVATE -ffunction-sections -fdata-sections)
set_property(TARGET recovery_blink_path_test APPEND_STRING PROPERTY LINK_FLAGS " -Wl,--gc-sections")
add_executable(recovery_lazy_fetch_test
    test_cases/recovery_priority/lazy_fetch_test.cc
    ../compute_server/server.cc
    ../compute_server/strategies/lazy.cc
    ../compute_server/strategies/eager.cc)
target_compile_options(recovery_lazy_fetch_test PRIVATE -ffunction-sections -fdata-sections)
set_property(TARGET recovery_lazy_fetch_test APPEND_STRING PROPERTY LINK_FLAGS " -Wl,--gc-sections")
foreach(recovery_target recovery_replay_wait_test recovery_storage_test recovery_blink_path_test recovery_lazy_fetch_test)
    target_link_libraries(${recovery_target}
        storage load_table core global ${BRPC_LIB}
        boost_coroutine boost_context boost_system ${DYNAMIC_LIB})
endforeach()
add_executable(recovery_observation_test test_cases/recovery_priority/observation_test.cc)
target_link_libraries(recovery_observation_test pthread)

# R2c C1: unified recovery-impact page catalog contract test (header-only lib)
add_executable(recovery_page_catalog_test test_cases/recovery_priority/recovery_page_catalog_test.cc)
target_link_libraries(recovery_page_catalog_test pthread)
add_test(NAME recovery_page_catalog_test COMMAND recovery_page_catalog_test)

# R3 C-1 (22.8): pluggable recovery policy interface contract test
add_executable(recovery_interface_contract_test
    test_cases/recovery_priority/recovery_interface_contract_test.cc
    ${PROJECT_SOURCE_DIR}/core/recovery_interface/recovery_interface.cc)
target_include_directories(recovery_interface_contract_test PRIVATE ${PROJECT_SOURCE_DIR})
target_link_libraries(recovery_interface_contract_test pthread)
add_test(NAME recovery_interface_contract_test COMMAND recovery_interface_contract_test)

# R1 read-only validator uses production ABI definitions and independent traversal.
add_executable(tree_stats tree_stats_main.cc)
target_link_libraries(tree_stats pthread ${OPENSSL_CRYPTO_LIBRARY})
find_program(RECOVERY_PYTHON python3)
if(RECOVERY_PYTHON)
    add_test(NAME recovery_scheduler_test COMMAND ${RECOVERY_PYTHON} -B
        ${CMAKE_CURRENT_SOURCE_DIR}/test_cases/recovery_priority/test_scheduler.py)
    add_test(NAME recovery_trace_contract_test COMMAND ${RECOVERY_PYTHON} -B
        ${CMAKE_CURRENT_SOURCE_DIR}/test_cases/recovery_priority/test_trace_contract.py)
    add_test(NAME recovery_e0_test COMMAND ${RECOVERY_PYTHON} -B
        ${PROJECT_SOURCE_DIR}/scripts/recovery/run_priority_pilot.py
        --binaries-dir ${CMAKE_CURRENT_BINARY_DIR}
        --output-root ${CMAKE_CURRENT_BINARY_DIR}/recovery-runs --functional-only)
    add_test(NAME tree_stats_selftest COMMAND ${RECOVERY_PYTHON} -B
        ${CMAKE_CURRENT_SOURCE_DIR}/test_cases/recovery_priority/test_tree_stats.py
        --checker $<TARGET_FILE:tree_stats>)
endif()
