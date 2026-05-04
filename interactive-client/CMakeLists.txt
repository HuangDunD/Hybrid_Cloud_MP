add_executable(InteractiveBench_client
    interactive_client.cc
)

target_link_libraries(InteractiveBench_client
    readline
)
