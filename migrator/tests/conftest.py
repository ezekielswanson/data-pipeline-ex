from pathlib import Path
import json
import logging
import pytest

from migrator.utils.logger import LoggerConfig

# TODO: Set this up when the config pack is finalized so that we can generate a real config pack

@pytest.fixture
def test_config_pack(tmp_path):
    """Create a temporary config pack for testing"""
    config_pack = tmp_path / "config-pack"
    config_pack.mkdir()
    
    # Create migration_config.json
    migration_config = {
        "source": {
            "type": "hubspot_api",
            "api_key": "${HUBSPOT_SOURCE_API_KEY}"
        },
        "target": {
            "type": "hubspot_api",
            "api_key": "${HUBSPOT_TARGET_API_KEY}"
        },
        "objects": {
            "contacts": {
                "config_path": "contacts",
                "batch_size": 100
            }
        }
    }
    
    with open(config_pack / "migration_config.json", "w") as f:
        json.dump(migration_config, f)
    
    # Create contacts directory with test configs
    contacts_dir = config_pack / "contacts"
    contacts_dir.mkdir()
    
    return config_pack


@pytest.fixture(autouse=True)
def reset_logger():
    """Reset logger between tests"""
    yield
    logger = logging.getLogger('migrator')
    logger.handlers.clear()
    LoggerConfig._instance = None
    LoggerConfig._initialized = False 