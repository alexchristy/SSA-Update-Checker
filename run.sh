#!/bin/bash

# Default working directory and virtual environment
WORKING_DIR="."
VENV_NAME=""
LOCK_FILE="/tmp/ssa-update-checker.lock"

# Function to display help message
show_help() {
    echo "Usage: $0 [OPTIONS]"
    echo
    echo "Available options:"
    echo "  -d, --working-directory DIR    Specify the working directory to change into"
    echo "  -e, --virtual-env ENV          Specify the virtual environment name in the working directory"
    echo "  -h, --help                     Show this help message and exit"
    echo
}

# Function to log a message to app.log with timestamp
log_message() {
    current_timestamp=$(date +"%Y-%m-%d %H:%M:%S,%3N")
    echo "$current_timestamp - $1" >> app.log
}

# Function to check and handle the lock file
handle_lock_file() {
    if [ -f "$LOCK_FILE" ]; then
        if [ -f "app.log" ]; then
            local log_dir="log"
            mkdir -p "$log_dir" # Creates the log directory if it does not exist, safely

            local current_time=$(date +"%H-%M-%S")
            local current_date=$(date +"%Y-%m-%d")

            mv app.log "${log_dir}/app_${current_time}_${current_date}.log"
        fi

        touch app.log
        log_message "Lock file $LOCK_FILE exists. Previous run may have failed or another instance is running."
        echo "Lock file $LOCK_FILE exists. Previous run may have failed or another instance is running."

        exit 1
    fi

    touch "$LOCK_FILE"
}

# Main function to encapsulate the logic
main() {
    handle_lock_file

    while [[ "$#" -gt 0 ]]; do
        case $1 in
            -d|--working-directory) WORKING_DIR="$2"; shift;;
            -e|--virtual-env) VENV_NAME="$2"; shift;;
            -h|--help) show_help; rm -f "$LOCK_FILE"; exit 0;;
            *) echo "Unknown parameter passed: $1"; show_help; rm -f "$LOCK_FILE"; exit 1;;
        esac
        shift
    done

    if [ -z "$VENV_NAME" ]; then
        echo "Please specify a virtual environment using -e or --virtual-env"
        show_help
        rm -f "$LOCK_FILE"
        exit 1
    fi

    cd "$WORKING_DIR" || { echo "Failed to change to directory $WORKING_DIR"; rm -f "$LOCK_FILE"; exit 1; }

    if ! source "$VENV_NAME/bin/activate"; then
        echo "Failed to activate virtual environment $VENV_NAME"
        rm -f "$LOCK_FILE"
        exit 1
    fi

    if ! python3.11 main.py --log INFO 2> error.tmp; then
        echo "======== ERRORS ========" >> app.log
        cat error.tmp >> app.log
        cat error.tmp
    fi

    rm -f error.tmp
    rm -f "$LOCK_FILE"

    local log_dir="log"
    mkdir -p "$log_dir"

    local current_time=$(date +"%H-%M-%S")
    local current_date=$(date +"%Y-%m-%d")

    if [ -f "app.log" ]; then
        mv app.log "${log_dir}/app_${current_time}_${current_date}.log"
    else
        echo "Warning: app.log does not exist."
    fi

    deactivate
}

# Execute the main function with all passed arguments
main "$@"
