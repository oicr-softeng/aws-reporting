#!/bin/bash

function environment_variable_error() {
    echo "The environment variables AWS_ACCESS_KEY and AWS_SECRET_KEY need to be defined"
    exit 1
}
# Check environment variables
[[ -z ${AWS_ACCESS_KEY} ]] && environment_variable_error
[[ -z ${AWS_SECRET_KEY} ]] && environment_variable_error

docker run -e AWS_ACCESS_KEY=$AWS_ACCESS_KEY -e AWS_SECRET_KEY=$AWS_SECRET_KEY oicrsofteng/aws-reporting bash report_runner.sh
