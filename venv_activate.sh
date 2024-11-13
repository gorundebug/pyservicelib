#!/bin/bash

original_dir=$(pwd)
path=$(dirname "$0")
cd "$path" || { echo "Error: Failed to change directory to script location"; exit 1; }

#python3 -m venv ~/python3.12
source  ~/python3.12/bin/activate

cd "$original_dir" || { echo "Error: Failed to change directory to '$original_dir'"; exit 1; }
