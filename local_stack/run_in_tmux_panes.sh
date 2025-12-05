#!/bin/bash

cd "$(dirname "$0")" || exit 1

./prepare.sh

PROJECT_ROOT=$(git rev-parse --show-toplevel)

# Create a new session with one window and one pane
tmux new-session -d -s horde -n "local_stack"

# Split into 5 panes
tmux split-window -h -t horde:local_stack.0
tmux split-window -v -t horde:local_stack.0
tmux split-window -v -t horde:local_stack.2
tmux split-window -v -t horde:local_stack.3

# Pane 0 (top-left): Facilitator
tmux send-keys -t horde:local_stack.0 "cd $PROJECT_ROOT/facilitator" C-m
tmux send-keys -t horde:local_stack.0 "../local_stack/tabs/0.facilitator.sh" C-m
tmux select-pane -t horde:local_stack.0 -T "Facilitator"

# Pane 1 (bottom-left): Facilitator Celery
tmux send-keys -t horde:local_stack.1 "cd $PROJECT_ROOT/facilitator" C-m
tmux send-keys -t horde:local_stack.1 "../local_stack/tabs/1.facilitator-celery.sh" C-m
tmux select-pane -t horde:local_stack.1 -T "FacilitatorCelery"

# Pane 2 (top-right): Miner
tmux send-keys -t horde:local_stack.2 "cd $PROJECT_ROOT/miner" C-m
tmux send-keys -t horde:local_stack.2 "../local_stack/tabs/2.miner.sh" C-m
tmux select-pane -t horde:local_stack.2 -T "Miner"

# Pane 3 (middle-right): ValidatorConnect
tmux send-keys -t horde:local_stack.3 "cd $PROJECT_ROOT/validator" C-m
tmux send-keys -t horde:local_stack.3 "../local_stack/tabs/3.validator-connect.sh" C-m
tmux select-pane -t horde:local_stack.3 -T "ValidatorConnect"

# Pane 4 (bottom-right): ValidatorCelery
tmux send-keys -t horde:local_stack.4 "cd $PROJECT_ROOT/validator" C-m
tmux send-keys -t horde:local_stack.4 "../local_stack/tabs/4.validator-celery.sh" C-m
tmux select-pane -t horde:local_stack.4 -T "ValidatorCelery"

tmux attach-session -t horde
