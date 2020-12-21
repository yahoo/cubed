#!/bin/bash

# The pid file location
pid_file=$(PID_FILE)

# Check if already running
if [ -f "$pid_file" ]
then
    # Get the pid
    read pid < $pid_file

    # Kill the process id from the pid file
    kill -9 $pid

    # Delete the pid file
    rm $pid_file
else
    echo "No running cubed, try running 'start_cubed' first."
fi
