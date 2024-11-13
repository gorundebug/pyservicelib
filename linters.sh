#!/bin/bash

original_dir=$(pwd)
path=$(dirname "$0")
cd "$path" || { echo "Error: Failed to change directory to script location"; exit 1; }

echo "pyright check"
pyright

echo "mypy check"
cd src/pyservicelib_gorundebug ||  { echo "Error: Failed to change directory to 'src/pyservicelib_gorundebug'"; exit 1; }
mypy -p pyservicelib_gorundebug --check-untyped-defs
cd ../..
mypy -p tests --check-untyped-defs --no-namespace-packages

echo "pytest"
pytest

cd "$original_dir" || { echo "Error: Failed to change directory to '$original_dir'"; exit 1; }
