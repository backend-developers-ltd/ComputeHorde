### WARNING! this attempts to kill all python processes!!!


# Recursively find all parent PIDs of a process
get_parents() {
  local pid=$1
  local parents=()
  while [ "$pid" != "1" ] && [ -n "$pid" ]; do
    pid=$(ps -o ppid= -p "$pid" | awk '{print $1}')
    if [ -n "$pid" ]; then
      parents+=("$pid")
    fi
  done
  echo "${parents[@]}"
}

# Recursively find all descendant PIDs of a process
get_descendants() {
  local pid=$1
  local children=$(pgrep -P "$pid")
  for child in $children; do
    children+=" $(get_descendants $child)"
  done
  echo "$children"
}

# Find all PIDs matching the "python", "celery"  and "tail" keywords
PIDS=$(pgrep -f '(^|/)(python([0-9.]*|w)?|celery|tail)([[:space:]]|$)' | sort -u)

# Iterate over each PID to find and kill the process, its parents, and its children
for pid in $PIDS; do
  # Get all parents, children, and combine them
  PARENTS=$(get_parents $pid)
  CHILDREN=$(get_descendants $pid)
  ALL_PROCESSES="$pid $PARENTS $CHILDREN"

  echo "Killing process tree for PID $pid: $ALL_PROCESSES"

  # Terminate all processes
  kill -9 $ALL_PROCESSES 2>/dev/null
done