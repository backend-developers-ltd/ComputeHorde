#!/bin/sh

echo "--- BEGIN ---"
date
find /volume | sort
echo "--- END---"

find /volume | sort > /output/files.txt
