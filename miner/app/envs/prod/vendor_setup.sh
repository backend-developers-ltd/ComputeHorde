#!/bin/sh

# Check if VENDOR_DIRECTORY is set
if [ -z "${VENDOR_DIRECTORY}" ]; then
  echo "VENDOR_DIRECTORY is not set."
else
  # Check if VENDOR_DIRECTORY exists
  if [ ! -d "${VENDOR_DIRECTORY}" ]; then
    echo "VENDOR_DIRECTORY does not exist: ${VENDOR_DIRECTORY}"
  else
    # Add VENDOR_DIRECTORY to PYTHONPATH
    export PYTHONPATH="${VENDOR_DIRECTORY}:${PYTHONPATH:-}"
    # Check if setup.sh exists in VENDOR_DIRECTORY
    if [ -f "${VENDOR_DIRECTORY}/setup.sh" ]; then
      # Run setup.sh
      . "${VENDOR_DIRECTORY}/setup.sh"
    fi
  fi
fi