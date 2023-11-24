#!/bin/bash

# Parse command-line arguments
option=$1
venv_path=${2:-"/kolla/asterixdb/jlw/pyvenv_dsprint"}

# Function to activate the virtual environment
activate_venv() {
  if [ -d "$venv_path" ]; then
    source "$venv_path/bin/activate"
    echo "Virtual environment activated: $venv_path"
  else
    echo "Virtual environment not found: $venv_path"
    exit 1
  fi
}

# Function to deactivate the virtual environment
deactivate_venv() {
  if [ -n "$VIRTUAL_ENV" ]; then
    deactivate
    echo "Virtual environment deactivated"
  else
    echo "No virtual environment currently activated"
  fi
}

# Function to print usage
print_usage() {
  echo "Usage: $0 [-activate|-a VENV_PATH] [-deactivate|-d]"
}

# Check the option and perform the corresponding action
case $option in
  -activate|-a)
    activate_venv
    ;;
  -deactivate|-d)
    deactivate_venv
    ;;
  *)
    print_usage
    ;;
esac

# Function to trap signals and deactivate the virtual environment
trap deactivate_venv SIGHUP SIGINT SIGTERM