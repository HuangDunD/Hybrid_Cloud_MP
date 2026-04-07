pkill -f test_multi_node.py
nohup bash -c 'python3 -u test_multi_node.py 2>&1 | awk '\''{ print strftime("[%Y-%m-%d %H:%M:%S]"), $0; fflush(); }'\''' > run.log &
