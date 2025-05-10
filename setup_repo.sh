#!/bin/bash

# Create main project directory (ensure we're in the right location)
cd /Users/musashi/Documents/CODE/GITHUB/OI-scraper

# Create source directory structure
mkdir -p src/scraper/{extractors,parsers,storage,utils}

# Create files in main scraper directory
touch src/scraper/__init__.py
touch src/scraper/constants.py
touch src/scraper/exceptions.py
touch src/scraper/main.py
touch src/scraper/config.py
touch src/scraper/types.py

# Create files in extractors directory
touch src/scraper/extractors/__init__.py
touch src/scraper/extractors/http_client.py
touch src/scraper/extractors/request_manager.py
touch src/scraper/extractors/selenium_client.py
touch src/scraper/extractors/browser_pool.py

# Create files in parsers directory
touch src/scraper/parsers/__init__.py
touch src/scraper/parsers/base_parser.py
touch src/scraper/parsers/trade_parser.py
touch src/scraper/parsers/table_parser.py
touch src/scraper/parsers/data_cleaner.py

# Create files in storage directory
touch src/scraper/storage/__init__.py
touch src/scraper/storage/database.py
touch src/scraper/storage/models.py
touch src/scraper/storage/schema.py
touch src/scraper/storage/query_builder.py

# Create files in utils directory
touch src/scraper/utils/__init__.py
touch src/scraper/utils/logging.py
touch src/scraper/utils/concurrency.py
touch src/scraper/utils/hash.py
touch src/scraper/utils/validation.py

# Create CLI entry point
touch src/cli.py

# Create test directory structure
mkdir -p tests/{unit/extractors,unit/parsers,unit/storage,integration,performance}

# Create test files
touch tests/__init__.py
touch tests/conftest.py

# Create unit test files
touch tests/unit/__init__.py
touch tests/unit/extractors/__init__.py
touch tests/unit/extractors/test_http_client.py
touch tests/unit/extractors/test_request_manager.py
touch tests/unit/parsers/__init__.py
touch tests/unit/parsers/test_trade_parser.py
touch tests/unit/parsers/test_data_cleaner.py
touch tests/unit/storage/__init__.py
touch tests/unit/storage/test_database.py
touch tests/unit/storage/test_models.py

# Create integration test files
touch tests/integration/__init__.py
touch tests/integration/test_scraper_flow.py
touch tests/integration/test_database_operations.py

# Create performance test files
touch tests/performance/__init__.py
touch tests/performance/test_parser_performance.py
touch tests/performance/test_database_performance.py
touch tests/performance/test_memory_usage.py

# Create config directory and files
mkdir -p config
touch config/default.yaml
touch config/development.yaml
touch config/production.yaml

# Create data directory structure
mkdir -p data/{raw,processed}
touch data/raw/.gitkeep
touch data/processed/.gitkeep

# Create logs directory
mkdir -p logs
touch logs/.gitkeep

# Create scripts directory
mkdir -p scripts
touch scripts/setup.py
touch scripts/run.py
touch scripts/benchmark.py
chmod +x scripts/setup.py
chmod +x scripts/run.py
chmod +x scripts/benchmark.py

# Create docs directory structure
mkdir -p docs/{api,usage,development}
touch docs/api/index.md
touch docs/usage/index.md
touch docs/development/index.md

# Create project files
touch .pre-commit-config.yaml
touch .gitignore
touch pyproject.toml
touch setup.cfg
touch README.md
touch CHANGELOG.md
touch setup.sh
chmod +x setup.sh

# Create basic .gitignore content
cat > .gitignore << 'EOF'
# Python
__pycache__/
*.py[cod]
*$py.class
*.so
.Python
.venv/
env/
venv/
ENV/
.pytest_cache/
.coverage
htmlcov/

# Data and logs
/data/raw/*
!/data/raw/.gitkeep
/data/processed/*
!/data/processed/.gitkeep
/logs/*
!/logs/.gitkeep

# Local config
.env
config/local.yaml

# IDE
.idea/
.vscode/
*.swp
*.swo

# OS
.DS_Store
Thumbs.db
EOF

# Create data directory .gitignore
cat > data/.gitignore << 'EOF'
# Ignore everything in this directory
*
# Except these files
!.gitignore
!.gitkeep
!*/
EOF

# Create logs directory .gitignore
cat > logs/.gitignore << 'EOF'
# Ignore everything in this directory
*
# Except these files
!.gitignore
!.gitkeep
EOF

# Initialize git repository
git init

# Display success message
echo "OpenInsider scraper repository successfully created at /Users/musashi/Documents/CODE/GITHUB/OI-scraper"
echo "Repository structure is now complete."