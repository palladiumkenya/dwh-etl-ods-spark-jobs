#!/usr/bin/env bash

echo "Clear Jars directory"
rm ./Jars/*.jar
echo "Copying builds to Jars directory..."
find ./ -type f -name "*jar-with-dependencies.jar" -exec cp -t Jars/ {} +