#!/bin/bash

echo "Setting up OpenInsider Scraper environment..."

# Check if Python 3.10+ is available
if ! command -v python3 &> /dev/null || ! python3 -c 'import sys; assert sys.version_info >= (3,10)' &> /dev/null; then
    echo "Python 3.10+ is required. Please install it and ensure 'python3' points to it."
    # Try 'python' as well
    if ! command -v python &> /dev/null || ! python -c 'import sys; assert sys.version_info >= (3,10)' &> /dev/null; then
        echo "Also checked 'python' command. Python 3.10+ not found."
        exit 1
    else
      PYTHON_CMD="python"
    fi
else
    PYTHON_CMD="python3"
fi
echo "Using $PYTHON_CMD"


# Check if Poetry is installed
if ! command -v poetry &> /dev/null; then
    echo "Poetry is not installed. Attempting to install Poetry..."
    # You might need to adjust this command depending on the OS or recommend manual installation.
    # This example uses the official recommended way that might require curl.
    if command -v curl &> /dev/null; then
        curl -sSL https://install.python-poetry.org | $PYTHON_CMD -
        # Add poetry to PATH. This depends on the OS and shell.
        # For bash/zsh, it's often:
        export PATH="$HOME/.local/bin:$PATH"
        echo "Poetry installed. Please source your shell profile (e.g., source ~/.bashrc) or open a new terminal."
        echo "Then, re-run this script or run 'poetry install' manually."
        exit 1 # Exit so user can restart shell or add to path
    else
        echo "curl is not installed. Please install Poetry manually from https://python-poetry.org/docs/#installation"
        exit 1
    fi
fi

echo "Poetry found. Version: $(poetry --version)"

# Create directories if they don't exist
echo "Creating necessary directories..."
mkdir -p data/raw
mkdir -p data/processed
mkdir -p logs

# Install dependencies using Poetry
echo "Installing project dependencies with Poetry..."
poetry install

if [ $? -ne 0 ]; then
    echo "Poetry install failed. Please check for errors."
    exit 1
fi

# (Optional) Install pre-commit hooks
if [ -f ".pre-commit-config.yaml" ]; then
    echo "Installing pre-commit hooks..."
    poetry run pre-commit install
    if [ $? -ne 0 ]; then
        echo "Failed to install pre-commit hooks. You can try running 'poetry run pre-commit install' manually."
    fi
else
    echo "No .pre-commit-config.yaml found, skipping pre-commit hook installation."
fi


echo "Setup complete!"
echo "To activate the virtual environment, run: poetry shell"
echo "Then you can run the scraper using: openinsider-cli --help"