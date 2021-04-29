#!/bin/sh

echo "=========Build Docker=============="
docker build -t dwh:latest .

echo "=========Run docker And  get result=========="
docker run --rm -v $PWD/data:/data -v $PWD/result:/result dwh:latest
