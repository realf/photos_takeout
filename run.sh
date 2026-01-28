#!/bin/sh

shopt -s nullglob
for file in *.zip; do 
    echo "Processing $file..."
    ditto -x -k "$file" . && \
    python3 process_google_takeout.py && \
    rm -rf "Takeout" || \
    { echo "Error processing $file"; break; }
done