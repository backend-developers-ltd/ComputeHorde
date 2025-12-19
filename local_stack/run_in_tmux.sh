#!/bin/bash

cd "$(dirname "$0")" || exit 1

./prepare.sh

PROJECT_ROOT=$(git rev-parse --show-toplevel)

tmux new-session -d -s horde -n "Facilitator"
tmux send-keys -t horde:Facilitator "cd $PROJECT_ROOT/facilitator" C-m
tmux send-keys -t horde:Facilitator "../local_stack/tabs/0.facilitator.sh" C-m

tmux new-window -t horde -n "FacilitatorCelery"
tmux send-keys -t horde:FacilitatorCelery "cd $PROJECT_ROOT/facilitator" C-m
tmux send-keys -t horde:FacilitatorCelery "../local_stack/tabs/1.facilitator-celery.sh" C-m

tmux new-window -t horde -n "Miner"
tmux send-keys -t horde:Miner "cd $PROJECT_ROOT/miner" C-m
tmux send-keys -t horde:Miner "../local_stack/tabs/2.miner.sh" C-m

tmux new-window -t horde -n "ValidatorConnect"
tmux send-keys -t horde:ValidatorConnect "cd $PROJECT_ROOT/validator" C-m
tmux send-keys -t horde:ValidatorConnect "../local_stack/tabs/3.validator-connect.sh" C-m

tmux new-window -t horde -n "ValidatorCelery"
tmux send-keys -t horde:ValidatorCelery "cd $PROJECT_ROOT/validator" C-m
tmux send-keys -t horde:ValidatorCelery "../local_stack/tabs/4.validator-celery.sh" C-m

tmux attach-session -t horde
