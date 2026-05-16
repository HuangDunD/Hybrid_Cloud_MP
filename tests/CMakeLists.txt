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

add_executable(affinity_migration_batch_test
    test_cases/affinity_migration_batch_test.cc
)

add_executable(affinity_migration_policy_test
    test_cases/affinity_migration_policy_test.cc
)

add_executable(page_lock_capacity_test
    test_cases/page_lock_capacity_test.cc
)

add_executable(interactive_affinity_runtime_test
    test_cases/interactive_affinity_runtime_test.cc
)

add_executable(interactive_parallel_fetch_test
    test_cases/interactive_parallel_fetch_test.cc
)

add_executable(router_protocol_send_test
    test_cases/router_protocol_send_test.cc
)

add_executable(interactive_socket_buffer_test
    test_cases/interactive_socket_buffer_test.cc
)

add_executable(fiber_stack_size_test
    test_cases/fiber_stack_size_test.cc
)

add_executable(interactive_init_thread_lightweight_test
    test_cases/interactive_init_thread_lightweight_test.cc
)

add_executable(sql_scheduler_exit_log_test
    test_cases/sql_scheduler_exit_log_test.cc
)

add_executable(storage_delete_replay_warning_test
    test_cases/storage_delete_replay_warning_test.cc
)

add_executable(blink_cache_hit_handle_cleanup_test
    test_cases/blink_cache_hit_handle_cleanup_test.cc
)

add_executable(affinity_timeseries_runtime_stats_test
    test_cases/affinity_timeseries_runtime_stats_test.cc
)

add_executable(affinity_process_memory_test
    test_cases/affinity_process_memory_test.cc
)

add_executable(affinity_partition_acceptance_test
    test_cases/affinity_partition_acceptance_test.cc
)

add_executable(affinity_assignment_target_test
    test_cases/affinity_assignment_target_test.cc
)

add_executable(affinity_schism_static_test
    test_cases/affinity_schism_static_test.cc
    ../core/affinity/assignment_table.cc
)

add_executable(lazy_remote_push_scheduler_test
    test_cases/lazy_remote_push_scheduler_test.cc
)

add_executable(lazy_local_lock_wait_test
    test_cases/lazy_local_lock_wait_test.cc
)
