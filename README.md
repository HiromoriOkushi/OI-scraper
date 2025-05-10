# OpenInsider.com Scraper

A high-performance, production-grade web scraper specifically for OpenInsider.com that focuses exclusively on efficient data extraction and storage.

## Features

- Extracts insider trading data from OpenInsider.com.
- Supports batch scraping and continuous monitoring.
- Stores data in an SQLite database.
- Handles rate limiting, retries, and potential site changes.
- Modular architecture for maintainability.

## Project Structure

(See prompt for directory structure)

## Setup

1.  **Clone the repository:**
    ```bash
    git clone <repository-url>
    cd openinsider-scraper
    ```

2.  **Install Poetry (if not already installed):**
    Follow instructions at [https://python-poetry.org/docs/#installation](https://python-poetry.org/docs/#installation)

3.  **Create a virtual environment and install dependencies:**
    ```bash
    poetry install
    ```
    Alternatively, use the provided `setup.sh` script:
    ```bash
    bash setup.sh
    ```

4.  **(Optional) Setup pre-commit hooks:**
    ```bash
    poetry run pre-commit install
    ```

## Configuration

Configuration is managed via YAML files in the `config/` directory.
- `default.yaml`: Base configuration.
- `development.yaml`: Overrides for development (merged with default).
- `production.yaml`: Overrides for production (merged with default).

The scraper uses `config/default.yaml` by default. You can specify an environment config:
`poetry run openinsider-cli --env development ...`
or a specific config file:
`poetry run openinsider-cli --config path/to/your/config.yaml ...`

## Usage

The scraper is controlled via a command-line interface. Activate the virtual environment first: `poetry shell`

**Perform a full scrape:**
```bash
openinsider-cli full