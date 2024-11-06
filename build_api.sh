#!/bin/bash

original_dir=$(pwd)
path=$(dirname "$0")
cd "$path" || { echo "Error: Failed to change directory to script location"; exit 1; }

rm -rf ./api/models

openapi-generator-cli generate -g python --skip-validate-spec -i ../servicelib/api/serviceapi.yaml --additional-properties packageName=pyservicelib.api --global-property models -o ./api

mv ./api/pyservicelib/api/models ./api/
rm -rf ./api/docs
rm -rf ./api/test
rm -rf ./api/pyservicelib

cd "$original_dir" || { echo "Error: Failed to change directory to '$original_dir'"; exit 1; }
