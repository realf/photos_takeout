#!/bin/sh

for file in *.zip; do
    [ -e "$file" ] || continue
    echo "Processing $file..."
    ditto -x -k "$file" . && \
    python3 process_google_takeout.py && \
    rm -rf "Takeout" || \
    { echo "Error processing $file"; break; }
done
