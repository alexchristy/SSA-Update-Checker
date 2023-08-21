#!/bin/bash

# Default working directory and virtual environment
WORKING_DIR="."
VENV_NAME=""
LOCK_FILE="/tmp/ssa-update-checker.lock"

show_help() {
    echo "Usage: $0 [OPTIONS]"
    echo
    echo "Available options:"
    echo "  -d, --working-directory DIR    Specify the working directory to change into"
    echo "  -e, --virtual-env ENV          Specify the virtual environment name in the working directory"
    echo "  -h, --help                     Show this help message and exit"
    echo
}

# Check for the presence of a lock file
if [ -f "$LOCK_FILE" ]; then
    echo "Lock file $LOCK_FILE exists. Previous run may have failed or another instance is running."
    exit 1
fi

# Create a lock file
touch "$LOCK_FILE"

# Parse arguments
while [[ "$#" -gt 0 ]]; do
    case $1 in
        -d|--working-directory) WORKING_DIR="$2"; shift;;
        -e|--virtual-env) VENV_NAME="$2"; shift;;
        -h|--help) show_help; rm -f "$LOCK_FILE"; exit 0;;
        *) echo "Unknown parameter passed: $1"; show_help; rm -f "$LOCK_FILE"; exit 1;;
    esac
    shift
done

# Ensure a virtual environment is specified
if [ -z "$VENV_NAME" ]; then
    echo "Please specify a virtual environment using -e or --virtual-env"
    show_help
    rm -f "$LOCK_FILE"
    exit 1
fi

# Change to the specified directory
cd "$WORKING_DIR" || { echo "Failed to change to directory $WORKING_DIR"; rm -f "$LOCK_FILE"; exit 1; }

# Activate the virtual environment
source "$VENV_NAME/bin/activate"

# Execute the python script and catch errors
{
    python3.11 main.py --log INFO
} 2>> error.tmp

# If the error.tmp file exists and is not empty, append its content to app.log
if [ -s "error.tmp" ]; then
    echo "======== ERRORS ========" >> app.log
    cat error.tmp >> app.log
    cat error.tmp
    rm error.tmp
    deactivate
    exit 1
fi

# Remove the lock file upon successful completion
rm -f "$LOCK_FILE"

# Remove when execution is done
rm -f error.tmp

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

# Deactivate the virtual environment
deactivate
