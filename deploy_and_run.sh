pkill -f test_multi_node.py
nohup bash -lc '
python3 -u test_multi_node.py 2>&1 | while IFS= read -r line; do
  printf "[%s] %s\n" "$(date "+%Y-%m-%d %H:%M:%S")" "$line"
done
' > run.log 2>&1 &
