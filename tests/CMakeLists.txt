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
