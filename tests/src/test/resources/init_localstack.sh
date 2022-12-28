#!/bin/sh

awslocal lambda create-function \
    --region eu-central-1 \
    --function-name echo \
    --runtime nodejs16.x \
    --handler index.handler \
    --memory-size 128 \
    --role "arn:aws:iam::123456:role/irrelevant" \
    --zip-file "fileb://function.zip"

echo "Initialized lambda function"