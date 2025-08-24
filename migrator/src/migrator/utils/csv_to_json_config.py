from typing import Dict, Any, List
from pathlib import Path
import pandas as pd
import json
import os
import importlib

# Import the registry dynamically to avoid circular imports
def get_transform_registry():
    """Dynamically import and return the TRANSFORM_REGISTRY"""
    module = importlib.import_module('migrator.transform.transform_methods')
    return getattr(module, 'TRANSFORM_REGISTRY')

def create_transform_config(row: pd.Series) -> Dict[str, Any]:
    """Create transformation configuration from row"""
    # Get registry to check for valid transformations
    registry = get_transform_registry()
    
    # Handle both string and boolean values for required
    if isinstance(row['required'], bool):
        required = row['required']
    else:
        required = str(row['required']).upper() == 'TRUE'
    
    config = {
        "required": required,
        "description": row['description']
    }
    
    # Process all columns dynamically
    for column, value in row.items():
        # Skip non-transformation columns
        if column in ['column_name', 'required', 'description']:
            continue
            
        # Handle boolean flags (transformations)
        if column in registry and pd.notna(value):
            if isinstance(value, bool):
                is_true = value
            else:
                is_true = str(value).upper() == 'TRUE'
                
            if is_true:
                config[column] = True
            continue
            
        # Handle special parameters
        if pd.notna(value) and value != "":
            if column == 'mapping_values':
                try:
                    config[column] = json.loads(value)
                except (json.JSONDecodeError, TypeError):
                    if isinstance(value, dict):
                        config[column] = value
                    else:
                        config[column] = value
            elif column == 'case_sensitive':
                if isinstance(value, bool):
                    config[column] = value
                else:
                    config[column] = str(value).upper() == 'TRUE'
            else:
                config[column] = value
    
    return config

def csv_to_json_config(csv_path: str, json_path: str):
    """Convert transformation CSV to JSON configuration"""
    df = pd.read_csv(csv_path)
    
    config = {
        "transformations": {
            "columns": {}
        }
    }
    
    for _, row in df.iterrows():
        config["transformations"]["columns"][row['column_name']] = create_transform_config(row)
    
    # Create directory if it doesn't exist
    os.makedirs(os.path.dirname(json_path), exist_ok=True)
    
    with open(json_path, 'w') as f:
        json.dump(config, f, indent=4)

def json_to_csv_config(json_path: str, csv_path: str):
    """Convert JSON configuration to CSV"""
    # Get registry to check for valid transformations
    registry = get_transform_registry()
    
    with open(json_path) as f:
        config = json.load(f)
    
    # Collect all possible columns from the config
    all_columns = set(['column_name', 'required', 'description'])
    
    # Add all registry keys as potential columns
    all_columns.update(registry.keys())
    
    # Add special parameters
    special_params = ['mapping_values', 'default_value', 'case_sensitive', 
                     'country_code', 'date_format', 'min_value', 'remove_suffixes']
    all_columns.update(special_params)
    
    # Collect all columns actually used in the config
    used_columns = set(['column_name', 'required', 'description'])
    
    rows = []
    for column_name, column_config in config['transformations']['columns'].items():
        row = {
            'column_name': column_name,
            'required': 'TRUE' if column_config.get('required', False) else 'FALSE',
            'description': column_config.get('description', '')
        }
        
        # Process all other config items
        for key, value in column_config.items():
            if key in ['required', 'description']:
                continue
                
            # Handle boolean flags (transformations)
            if key in registry and isinstance(value, bool):
                row[key] = 'TRUE' if value else 'FALSE'
                used_columns.add(key)
            # Handle mapping values
            elif key == 'mapping_values':
                if isinstance(value, dict):
                    row[key] = json.dumps(value)
                else:
                    row[key] = value
                used_columns.add(key)
            # Handle boolean parameters
            elif key == 'case_sensitive':
                row[key] = 'TRUE' if value else 'FALSE'
                used_columns.add(key)
            # Handle all other parameters
            elif value is not None:
                row[key] = value
                used_columns.add(key)
        
        rows.append(row)
    
    # Create DataFrame with only the columns that are actually used
    df = pd.DataFrame(rows, columns=[col for col in all_columns if col in used_columns])
    
    # Convert boolean values to strings
    for col in df.columns:
        if df[col].dtype == bool:
            df[col] = df[col].map({True: 'TRUE', False: 'FALSE'})
    
    # Ensure directory exists
    os.makedirs(os.path.dirname(csv_path), exist_ok=True)
    
    # Write CSV
    df.to_csv(csv_path, index=False)

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) < 3:
        print("Usage: python csv_to_json_config.py <input_file> <output_file>")
        sys.exit(1)
    
    input_file = sys.argv[1]
    output_file = sys.argv[2]
    
    if input_file.endswith('.csv'):
        csv_to_json_config(input_file, output_file)
        print(f"Converted {input_file} to {output_file}")
    elif input_file.endswith('.json'):
        json_to_csv_config(input_file, output_file)
        print(f"Converted {input_file} to {output_file}")
    else:
        print("Input file must be .csv or .json")
        sys.exit(1)