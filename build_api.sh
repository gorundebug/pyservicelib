#!/bin/bash

original_dir=$(pwd)
path=$(dirname "$0")
cd "$path" || { echo "Error: Failed to change directory to script location"; exit 1; }

rm -rf ./src/pyservicelib_gorundebug/api/models

openapi-generator generate -g python --skip-validate-spec -i ../servicelib/api/serviceapi.yaml --additional-properties packageName=. --global-property models -o ./src/pyservicelib_gorundebug/api

rm -rf ./src/pyservicelib_gorundebug/api/docs
rm -rf ./src/pyservicelib_gorundebug/api/test

touch ./src/pyservicelib_gorundebug/api/__init__.py
touch ./src/pyservicelib_gorundebug/api/models/__init__.py

cd "$original_dir" || { echo "Error: Failed to change directory to '$original_dir'"; exit 1; }
