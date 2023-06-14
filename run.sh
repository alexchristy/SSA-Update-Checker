#!/bin/bash

# This script is used to run the frontend and backend servers in order.

# Run the backend server to populate the database
python3 main.py &

# Run the frontend server
python3 telegram_messenger.py