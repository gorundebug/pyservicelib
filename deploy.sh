#!/bin/bash

original_dir=$(pwd)
path=$(dirname "$0")
cd "$path" || { echo "Error: Failed to change directory to script location"; exit 1; }

bash build.sh
python3 -m twine upload --repository testpypi dist/*

cd "$original_dir" || { echo "Error: Failed to change directory to '$original_dir'"; exit 1; }
