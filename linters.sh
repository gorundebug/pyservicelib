#!/bin/bash

original_dir=$(pwd)
path=$(dirname "$0")
cd "$path" || { echo "Error: Failed to change directory to script location"; exit 1; }
echo "pyright check"
pyright
echo "mypy check"
mypy -p pyservicelib --check-untyped-defs

pytest

cd "$original_dir" || { echo "Error: Failed to change directory to '$original_dir'"; exit 1; }
