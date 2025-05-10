# src/scraper/config.py
import yaml
from pathlib import Path
from typing import Dict, Any, Optional
import logging

from .exceptions import ConfigurationError

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent

def deep_merge_dicts(base: Dict, update: Dict) -> Dict:
    """Recursively merge two dictionaries. Update values override base values."""
    merged = base.copy()
    for key, value in update.items():
        if isinstance(value, dict) and key in merged and isinstance(merged[key], dict):
            merged[key] = deep_merge_dicts(merged[key], value)
        else:
            merged[key] = value
    return merged

def load_config(config_path: Optional[str] = None, env: Optional[str] = None) -> Dict[str, Any]:
    """
    Loads configuration from YAML files.
    It loads a default config, then merges an optional environment-specific config.
    """
    if config_path:
        primary_config_file = Path(config_path)
        if not primary_config_file.is_absolute():
            primary_config_file = PROJECT_ROOT / primary_config_file
    else:
        primary_config_file = PROJECT_ROOT / "config/default.yaml"

    if not primary_config_file.exists():
        raise ConfigurationError(f"Primary configuration file not found: {primary_config_file}")

    try:
        with open(primary_config_file, 'r') as f:
            config_data = yaml.safe_load(f)
            if not isinstance(config_data, dict): # Ensure it's a dictionary
                raise ConfigurationError(f"Configuration file {primary_config_file} is not a valid YAML dictionary.")
    except yaml.YAMLError as e:
        raise ConfigurationError(f"Error parsing YAML configuration from {primary_config_file}: {e}")
    except IOError as e:
        raise ConfigurationError(f"Error reading configuration file {primary_config_file}: {e}")

    env_config_file = None
    if env and env != "default":
        # Construct path to environment-specific config, e.g., config/development.yaml
        potential_env_config_path = primary_config_file.parent / f"{env}.yaml"
        if potential_env_config_path.exists():
            env_config_file = potential_env_config_path
        else:
            logger.warning(f"Environment configuration file for '{env}' not found at {potential_env_config_path}. Using primary config only.")
            
    elif not config_path and not env: # Defaulting scenario if no specific path or env given
        # This attempts to load development.yaml by default if it exists, on top of default.yaml
        # This logic can be adjusted based on desired default behavior
        potential_dev_config = primary_config_file.parent / "development.yaml"
        if potential_dev_config.exists():
            logger.info(f"No specific environment set, development.yaml found, merging it.")
            env_config_file = potential_dev_config


    if env_config_file:
        try:
            with open(env_config_file, 'r') as f:
                env_config_data = yaml.safe_load(f)
                if not isinstance(env_config_data, dict):
                    raise ConfigurationError(f"Environment configuration file {env_config_file} is not a valid YAML dictionary.")
                config_data = deep_merge_dicts(config_data, env_config_data)
                logger.info(f"Loaded and merged environment configuration from: {env_config_file}")
        except yaml.YAMLError as e:
            raise ConfigurationError(f"Error parsing YAML from {env_config_file}: {e}")
        except IOError as e:
            raise ConfigurationError(f"Error reading {env_config_file}: {e}")
    
    # Ensure essential paths are absolute
    if 'database' in config_data and 'path' in config_data['database']:
        db_path = Path(config_data['database']['path'])
        if not db_path.is_absolute():
            config_data['database']['path'] = str(PROJECT_ROOT / db_path)

    if 'logging' in config_data and 'file' in config_data['logging']:
        log_path = Path(config_data['logging']['file'])
        if not log_path.is_absolute():
            config_data['logging']['file'] = str(PROJECT_ROOT / log_path)
            
    if 'advanced' in config_data and 'caching' in config_data['advanced'] and \
       'cache_name' in config_data['advanced']['caching']:
        cache_db_path = Path(config_data['advanced']['caching']['cache_name'])
        if not cache_db_path.is_absolute():
             config_data['advanced']['caching']['cache_name'] = str(PROJECT_ROOT / cache_db_path)
             # Ensure directory exists for sqlite cache
             if config_data['advanced']['caching']['backend'] == 'sqlite':
                 Path(config_data['advanced']['caching']['cache_name']).parent.mkdir(parents=True, exist_ok=True)


    logger.debug(f"Final configuration loaded: {config_data}")
    return config_data