#!/bin/bash

# Check if no arguments are provided
if [ "$#" -eq 0 ]; then
    echo "Usage: $0 [list of files]"
    exit 1
fi

# Iterate over each file argument
for file in "$@"; do
    # Get the parent directory of each file
    dir=$(dirname "$file")
    # Get only the immediate parent directory
    parent_dir=$(basename "$dir")
    # Print the unique parent directory
    echo "$parent_dir"
done | sort -u