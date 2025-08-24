from pathlib import Path
import json
import pandas as pd
from dotenv import load_dotenv
import os
from typing import Dict, Any

class ConfigPackLoader:
    def __init__(self, config_pack_path: str = 'config/config-pack'):
        self.config_pack_path = Path(config_pack_path)
        self.validate_config_pack()
        
        # Load environment variables
        load_dotenv()
        
        # Load main configuration
        self.config = self._load_migration_config()
        
    def validate_config_pack(self):
        """Ensure config pack has required structure"""
        if not self.config_pack_path.exists():
            raise ValueError(f"Config pack not found at {self.config_pack_path}")
            
        if not (self.config_pack_path / 'migration_config.json').exists():
            raise ValueError("migration_config.json not found in config pack")
            
    
    def _load_migration_config(self) -> Dict[str, Any]:
        """Load and process main configuration file"""
        with open(self.config_pack_path / 'migration_config.json') as f:
            config = json.load(f)
            
        # Process environment variables
        config['source']['api_key'] = os.getenv(config['source']['api_key'].strip('${}'))
        config['target']['api_key'] = os.getenv(config['target']['api_key'].strip('${}'))
        
        return config
    
    def load_object_config(self, object_type: str) -> Dict[str, Any]:
        """Load all configuration for a specific object type"""
        if object_type not in self.config['objects']:
            raise ValueError(f"No configuration found for object type: {object_type}")
            
        object_path = self.config_pack_path / object_type
        
        # Load transformations
        with open(object_path / 'transformations.json') as f:
            transformations = json.load(f)
            
        # Load mappings
        mappings = pd.read_csv(object_path / 'mappings.csv')
        
        return {
            'base_config': self.config['objects'][object_type],
            'transformations': transformations,
            'mappings': mappings
        }
    
    def get_execution_config(self) -> Dict[str, Any]:
        """Get execution-related configuration"""
        return self.config.get('execution', {})
    
    def get_logging_config(self) -> Dict[str, Any]:
        """Get logging-related configuration"""
        return self.config.get('logging', {})