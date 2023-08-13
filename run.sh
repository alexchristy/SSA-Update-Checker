#!/bin/bash

# Default working directory
WORKING_DIR="."

show_help() {
    echo "Usage: $0 [OPTIONS]"
    echo
    echo "Available options:"
    echo "  -d, --working-directory DIR    Specify the working directory to change into"
    echo "  -h, --help                     Show this help message and exit"
    echo
}

# Parse arguments
while [[ "$#" -gt 0 ]]; do
    case $1 in
        -d|--working-directory) WORKING_DIR="$2"; shift;;
        -h|--help) show_help; exit 0;;
        *) echo "Unknown parameter passed: $1"; show_help; exit 1;;
    esac
    shift
done

# Change to the specified directory
cd "$WORKING_DIR" || { echo "Failed to change to directory $WORKING_DIR"; exit 1; }

# Execute the python script and catch errors
{
    python3 main.py --log INFO
} 2>> error.tmp

# If the error.tmp file exists and is not empty, append its content to app.log
if [ -s "error.tmp" ]; then
    echo "======== ERRORS ========" >> app.log
    cat error.tmp >> app.log
fi

# Remove error.tmp when done
rm error.tmp

# Check if log directory exists, if not create it
[ -d "log" ] || mkdir log

# Get current time and date
CURRENT_TIME=$(date +"%H:%M:%S")
CURRENT_DATE=$(date +"%m-%d-%y")

# Rename and move the log file
if [ -f "app.log" ]; then
    mv app.log "log/app_${CURRENT_TIME}_${CURRENT_DATE}.log"
else
    echo "Warning: app.log does not exist."
fi