#!/bin/bash

original_dir=$(pwd)
path=$(dirname "$0")
cd "$path" || { echo "Error: Failed to change directory to script location"; exit 1; }

VENV=".venv/bin"

echo "pyright check"
npx --yes pyright

echo "mypy check"
cd src || { echo "Error: Failed to change directory to 'src'"; exit 1; }
"../$VENV/mypy" -p pyservicelib_gorundebug --check-untyped-defs
cd ..
"$VENV/mypy" -p tests --check-untyped-defs --no-namespace-packages

echo "pytest"
"$VENV/pytest"

cd "$original_dir" || { echo "Error: Failed to change directory to '$original_dir'"; exit 1; }
