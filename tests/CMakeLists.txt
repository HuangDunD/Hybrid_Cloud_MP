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

add_executable(page_lock_capacity_test
    test_cases/page_lock_capacity_test.cc
)

add_executable(interactive_affinity_runtime_test
    test_cases/interactive_affinity_runtime_test.cc
)
