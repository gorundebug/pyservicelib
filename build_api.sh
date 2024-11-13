#!/bin/bash

original_dir=$(pwd)
path=$(dirname "$0")
cd "$path" || { echo "Error: Failed to change directory to script location"; exit 1; }

rm -rf ./pyservicelib/api/models

openapi-generator-cli generate -g python --skip-validate-spec -i ../servicelib/api/serviceapi.yaml --additional-properties packageName=. --global-property models -o ./pyservicelib/api

rm -rf ./pyservicelib/api/docs
rm -rf ./pyservicelib/api/test

touch ./pyservicelib/api/__init__.py
touch ./pyservicelib/api/models/__init__.py

cd "$original_dir" || { echo "Error: Failed to change directory to '$original_dir'"; exit 1; }
