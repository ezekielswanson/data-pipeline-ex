import pandas as pd
from pathlib import Path
from migrator.transform.transform import DataTransformer
from migrator.utils.csv_to_json_config import csv_to_json_config, json_to_csv_config
import numpy as np


def test_config_files():
    """Test that the configuration files work with our implementation"""
    # Paths to config files
    config_dir = Path("src/migrator/config/config-pack/contacts")
    json_path = config_dir / "transformations.json"
    csv_path = config_dir / "transformations.csv"
    
    # Test JSON to CSV conversion
    output_csv = config_dir / "test_output.csv"
    json_to_csv_config(str(json_path), str(output_csv))
    
    # Test CSV to JSON conversion
    output_json = config_dir / "test_output.json"
    csv_to_json_config(str(csv_path), str(output_json))
    
    # Load JSON config and create transformer
    with open(json_path) as f:
        import json
        config = json.load(f)
    
    transformer = DataTransformer(config)
    
    # Create test data
    data = {
        "email": [" Test@Example.com ", "john.doe@gmail.com", None],
        "phone": ["(555) 123-4567", "555.987.6543", None],
        "first_name": ["john", "jane", "robert"],
        "last_name": ["smith", "doe", "johnson"],
        "company": ["Acme, INC", "Tech LLC", "Global ltd"],
        "website": ["www.example.com", "invalid-url", None],
        "lead_status": ["New", "In Progress", "Unknown"],
        "created_date": ["2023-01-15", "2023-02-20", "2023-03-25"],
        "annual_revenue": ["1000", "-500", "not-a-number"],
        "hs_object_id": [1234.0, 5678.0, 9012.0]
    }
    
    df = pd.DataFrame(data)
    
    # Transform the data
    result = transformer.transform_dataframe(df)
    
    # Verify basic transformations
    assert result["email"][0] == "test@example.com"
    assert result["phone"][0] == "+15551234567"
    assert result["first_name"][0] == "John"
    assert result["last_name"][0] == "Smith"
    assert result["lead_status"][0] == "Open"  # Mapped from "New"
    assert isinstance(result["hs_object_id"][0], int) or (
        hasattr(result["hs_object_id"][0], "item") and 
        isinstance(result["hs_object_id"][0].item(), int)
    )
    
    # Verify enhanced transformations (if configured)
    # These assertions should be conditional based on whether the enhancements are in the config
    if config["transformations"]["columns"]["email"].get("normalize_gmail", False):
        assert "johndoe@gmail.com" == result["email"][1]  # Gmail dots removed
    
    if config["transformations"]["columns"]["company"].get("case_insensitive", False):
        assert "Acme" == result["company"][0]  # INC removed case-insensitively
    
    if config["transformations"]["columns"]["website"].get("remove_www", False):
        assert not result["website"][0].startswith("www.")  # www removed
    
    # Clean up test files
    if output_csv.exists():
        output_csv.unlink()
    if output_json.exists():
        output_json.unlink()
    
    print("Configuration files test passed!")

if __name__ == "__main__":
    test_config_files() 