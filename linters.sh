#!/bin/bash

original_dir=$(pwd)
path=$(dirname "$0")
cd "$path" || { echo "Error: Failed to change directory to script location"; exit 1; }
cd ..
path=$(pwd)
echo "pyright check"
pyright
echo "mypy check"
mypy -p pyservicelib --check-untyped-defs
cd "./pyservicelib" || exit
PYTHONPATH="$path" pytest

cd "$original_dir" || { echo "Error: Failed to change directory to '$original_dir'"; exit 1; }
