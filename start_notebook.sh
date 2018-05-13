#!/usr/bin/env bash

# Set & move to home directory
source set_env.sh
cd "$HOLOCLEANHOME"

# Launch jupyter notebook!
echo "Launching Jupyter Notebook..."
jupyter notebook
