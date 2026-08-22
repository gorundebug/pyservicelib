#!/bin/bash

original_dir=$(pwd)
path=$(dirname "$0")
cd "$path" || { echo "Error: Failed to change directory to script location"; exit 1; }

pip3 install -i https://test.pypi.org/simple/ --extra-index-url https://pypi.org/simple/  pyservicelib-gorundebug==0.0.1

cd "$original_dir" || { echo "Error: Failed to change directory to '$original_dir'"; exit 1; }
