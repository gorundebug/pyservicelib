#
# Copyright (c) 2024 Sergey Alexeev
# Email: sergeyalexeev@yahoo.com
#
#  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
#

cd "$(dirname "$0")" || { echo "Error: Failed to change directory to script location"; exit 1; }

rm -rf ./api/models

openapi-generator-cli generate -g python --skip-validate-spec -i ../servicelib/api/serviceapi.yaml --additional-properties packageName=pyservicelib.api --global-property models -o ./api

mv ./api/pyservicelib/api/models ./api/
rm -rf ./api/docs
rm -rf ./api/test
rm -rf ./api/pyservicelib
