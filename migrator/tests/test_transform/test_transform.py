import os
import sys
import pandas as pd
import json
from pathlib import Path
import pytest
import tempfile
import shutil

# Updated import syntax following PR discussion: https://github.com/Aptitude-8/acrisure-migrator/pull/29#discussion_r1965537446 
import migrator.transform.transform as transform_module
import migrator.transform.transform_methods as transform_methods
import migrator.utils.csv_to_json_config as config_utils

# specific functions, classes, objects 
DataTransformer = transform_module.DataTransformer
TRANSFORM_REGISTRY = transform_methods.TRANSFORM_REGISTRY
csv_to_json_config = config_utils.csv_to_json_config
json_to_csv_config = config_utils.json_to_csv_config

@pytest.fixture
def test_data_dir():
    # temp directory for test data 
    temp_dir = tempfile.mkdtemp()
    yield Path(temp_dir)
    shutil.rmtree(temp_dir)

@pytest.fixture
def test_config_csv(test_data_dir):
    # test csv config
    csv_path = test_data_dir / "test_config.csv"
    
    # Create DataFrame with test data
    data = {
        "column_name": ["email", "phone", "status"],
        "required": ["TRUE", "FALSE", "TRUE"],
        "description": ["Email address", "Phone number", "Status"],
        "trim": ["TRUE", "TRUE", "TRUE"],
        "lowercase": ["TRUE", "FALSE", "FALSE"],
        "uppercase": ["FALSE", "FALSE", "FALSE"],
        "titlecase": ["FALSE", "FALSE", "FALSE"],
        "phone": ["FALSE", "TRUE", "FALSE"],
        "email": ["TRUE", "FALSE", "FALSE"],
        "date": ["FALSE", "FALSE", "FALSE"],
        "mapping_values": ["", "", '{"Active":"1","Inactive":"0","*":"Unknown"}'],
        "default_value": ["", "", "Unknown"]
    }
    
    df = pd.DataFrame(data)
    df.to_csv(csv_path, index=False)
    
    return csv_path

@pytest.fixture
def test_config_json(test_data_dir):
    # JSON to CSV config test
    json_path = test_data_dir / "test_config.json"
    
    config = {
        "transformations": {
            "columns": {
                "email": {
                    "required": True,
                    "description": "Primary company email",
                    "trim": True,
                    "lowercase": True,
                    "email": True,
                    "normalize_gmail": True,
                    "validate": True
                },
                "phone": {
                    "required": False,
                    "description": "Phone number",
                    "trim": True,
                    "phone": True
                },
                "status": {
                    "required": True,
                    "description": "Status",
                    "trim": True,
                    "mapping_values": {"Active": "1", "Inactive": "0", "*": "Unknown"},
                    "default_value": "Unknown"
                },
                "website": {
                    "required": False,
                    "description": "Company website",
                    "trim": True,
                    "lowercase": True,
                    "url": True,
                    "validate": True,
                    "remove_www": True
                },
                "company_name": {
                    "required": True,
                    "description": "Company legal name",
                    "trim": True,
                    "titlecase": True,
                    "company": True,
                    "remove_suffixes": "Inc,LLC,Ltd,Corp,Corporation",
                    "case_insensitive": True
                }
            }
        }
    }
    
    with open(json_path, 'w') as f:
        json.dump(config, f, indent=4)
    
    return json_path

def test_csv_to_json_conversion(test_data_dir, test_config_csv):
    """Test conversion from CSV to JSON configuration"""
    json_path = test_data_dir / "output_config.json"
    
    # CSV to json
    csv_to_json_config(str(test_config_csv), str(json_path))
    
    # Verify JSON content
    with open(json_path) as f:
        config = json.load(f)
    
    assert "transformations" in config
    assert "columns" in config["transformations"]
    assert "email" in config["transformations"]["columns"]
    assert "phone" in config["transformations"]["columns"]
    assert "status" in config["transformations"]["columns"]
    
    email_config = config["transformations"]["columns"]["email"]
    assert email_config["required"] == True
    assert email_config["trim"] == True
    assert email_config["lowercase"] == True
    assert email_config["email"] == True
    
    status_config = config["transformations"]["columns"]["status"]
    assert status_config["required"] == True
    assert status_config["trim"] == True
    assert "mapping_values" in status_config
    assert status_config["default_value"] == "Unknown"

def test_json_to_csv_conversion(test_data_dir, test_config_json):
#   JSON to CSV config test
    csv_path = test_data_dir / "output_config.csv"
    
    # Convert JSON to CSV
    json_to_csv_config(str(test_config_json), str(csv_path))
    
    # Verify CSV content
    df = pd.read_csv(csv_path)
    
    assert "column_name" in df.columns
    assert "trim" in df.columns
    assert "lowercase" in df.columns
    assert "email" in df.column_name.values
    assert "phone" in df.column_name.values
    assert "status" in df.column_name.values
    
    email_row = df[df.column_name == "email"].iloc[0]
    # Convert to string for comparison or check boolean value
    assert str(email_row["trim"]).upper() == "TRUE" or email_row["trim"] == True
    assert str(email_row["lowercase"]).upper() == "TRUE" or email_row["lowercase"] == True
    assert str(email_row["email"]).upper() == "TRUE" or email_row["email"] == True
    
    status_row = df[df.column_name == "status"].iloc[0]
    assert str(status_row["trim"]).upper() == "TRUE" or status_row["trim"] == True
    assert "mapping_values" in df.columns
    assert status_row["default_value"] == "Unknown"

def test_data_transformation():
    # Create a test configuration
    config = {
        "transformations": {
            "columns": {
                "email": {
                    "required": True,
                    "description": "Email address",
                    "trim": True,
                    "lowercase": True
                },
                "phone": {
                    "required": False,
                    "description": "Phone number",
                    "trim": True,
                    "country_code": "1"
                },
                "name": {
                    "required": True,
                    "description": "Full name",
                    "trim": True,
                    "titlecase": True
                },
                "status": {
                    "required": True,
                    "description": "Status",
                    "trim": True,
                    "mapping_values": {"Active": "1", "Inactive": "0", "*": "Unknown"},
                    "default_value": "Unknown",
                    "case_sensitive": False
                }
            }
        }
    }
    
    # Create a test dataframe
    data = {
        "email": [" Test@Example.com ", "another@test.com", None],
        "phone": ["(555) 123-4567", "555.987.6543", None],
        "name": ["john smith", " JANE DOE ", "robert johnson"],
        "status": ["Active", "inactive", "unknown"]
    }
    
    df = pd.DataFrame(data)
    
    # Transform the data
    transformer = DataTransformer(config)
    
    # print("\nDEBUG: Test data before transformation:")
    # print(df.head())
    
    result = transformer.transform_dataframe(df)
    
    # print("\nDEBUG: Test data after transformation:")
    # print(result.head())
    
    # Verify transformations
    assert result["email"][0] == "test@example.com"
    assert result["phone"][0] == "+15551234567"
    assert result["name"][0] == "John Smith"
    assert result["status"][0] == "1"  # Mapped from "Active"
    assert result["status"][1] == "0"  # Mapped from "inactive" (case insensitive)
    assert result["status"][2] == "Unknown"  # Default value

def test_enhanced_transformations():
    """Test the enhanced transformation functions with automatic features"""
    config = {
        "transformations": {
            "columns": {
                "email": {
                    "required": True,
                    "description": "Email address",
                    "trim": True,
                    "email": True,
                    "validate": True
                },
                "company": {
                    "required": True,
                    "description": "Company name",
                    "trim": True,
                    "company": True,
                    "remove_suffixes": "Inc,LLC,Ltd"
                },
                "url": {
                    "required": False,
                    "description": "Website URL",
                    "trim": True,
                    "url": True,
                    "validate": True
                },
                "number": {
                    "required": False,
                    "description": "Numeric value",
                    "number": True,
                    "min_value": 10,
                    "max_value": 100,
                    "default_value": 50
                }
            }
        }
    }
    
    # Create a test dataframe
    data = {
        "email": ["john.doe@gmail.com", "invalid@", None],
        "company": ["Acme, INC", "Tech LLC", "Global ltd"],
        "url": ["www.example.com", "invalid", None],
        "number": ["5", "150", None]
    }
    
    df = pd.DataFrame(data)
    
    # Transform the data
    transformer = DataTransformer(config)
    result = transformer.transform_dataframe(df)
    
    # Verify automatic Gmail normalization
    assert result["email"][0] == "johndoe@gmail.com"  # Dots removed
    assert pd.isna(result["email"][1])  # Invalid email returns None when validate=True
    
    # Verify automatic case-insensitive suffix removal
    assert result["company"][0].strip(", ") == "Acme"  # INC removed case-insensitively, strip trailing comma and space
    assert result["company"][1].strip(", ") == "Tech"  # LLC removed
    assert result["company"][2].strip(", ") == "Global"  # ltd removed
    
    # Verify automatic www removal
    assert not str(result["url"][0]).startswith("www.")  # www removed
    assert pd.isna(result["url"][1])  # Invalid URL returns None when validate=True
    
    # Verify number validation
    assert result["number"][0] == 50  # Below min_value, so default_value is used
    assert result["number"][1] == 100  # Above max_value, so max_value is used

def test_registry_integration():
    assert "trim" in TRANSFORM_REGISTRY
    assert "lowercase" in TRANSFORM_REGISTRY
    assert "uppercase" in TRANSFORM_REGISTRY
    assert "titlecase" in TRANSFORM_REGISTRY
    assert "phone" in TRANSFORM_REGISTRY
    assert "email" in TRANSFORM_REGISTRY
    
    assert TRANSFORM_REGISTRY["trim"](" test ") == "test"
    assert TRANSFORM_REGISTRY["lowercase"]("TEST") == "test"
    assert TRANSFORM_REGISTRY["uppercase"]("test") == "TEST"
    assert TRANSFORM_REGISTRY["titlecase"]("john smith") == "John Smith"

def test_transform_methods():
    # Test trim
    assert transform_methods.trim(" test ") == "test"
    assert transform_methods.trim(None) is None
    
    # Test lowercase
    assert transform_methods.lowercase("TEST") == "test"
    assert transform_methods.lowercase(None) is None
    
    # Test uppercase
    assert transform_methods.uppercase("test") == "TEST"
    assert transform_methods.uppercase(None) is None
    
    # Test titlecase
    assert transform_methods.titlecase("john smith") == "John Smith"
    assert transform_methods.titlecase(None) is None
    
    # Test phone with kwargs
    assert transform_methods.phone("(555) 123-4567", country_code="1") == "+15551234567"
    assert transform_methods.phone(None) is None
    
    # Test email with automatic Gmail normalization
    assert transform_methods.email("john.doe@gmail.com") == "johndoe@gmail.com"
    assert transform_methods.email("regular@example.com") == "regular@example.com"
    
    # Test date with kwargs
    assert transform_methods.date("2023-01-15", date_format="%m/%d/%Y") == "01/15/2023"
    assert transform_methods.date(None) is None
    
    # Test company with automatic case-insensitive matching
    assert transform_methods.company("Acme, INC.") == "Acme"
    
    # Test URL with automatic www removal
    assert transform_methods.url("www.example.com", validate=True) == "https://example.com"
    
    # Test number with kwargs
    assert transform_methods.number("1,234.56") == 1234.56
    assert transform_methods.number("5", min_value="10", default_value="0") == 0.0
    assert transform_methods.number(None) is None
    
    # Test enhanced number with max_value
    assert transform_methods.number("150", min_value="10", max_value="100", default_value="50") == 100

def test_falsey_value_handling():
    config = {
        "transformations": {
            "columns": {
                "email": {
                    "required": True,
                    "description": "Email address",
                    "trim": True,
                    "lowercase": True
                },
                "name": {
                    "required": True,
                    "description": "Full name",
                    "trim": True,
                    "titlecase": True
                }
            }
        }
    }
    

    data = {
        "email": ["test@example.com", "", "null", "  ", "undefined", "NONE"],
        "name": ["John Smith", "null", "", "   ", "undefined", "none"]
    }
    
    df = pd.DataFrame(data)
    
    # Transform the data
    transformer = DataTransformer(config)
    result = transformer.transform_dataframe(df)
    
    # Verify transformations
    assert result["email"][0] == "test@example.com"  # Normal value
    assert pd.isna(result["email"][1])  # Empty string
    assert pd.isna(result["email"][2])  # "null"
    assert pd.isna(result["email"][3])  # Whitespace
    assert pd.isna(result["email"][4])  # "undefined"
    assert pd.isna(result["email"][5])  # "NONE"
    
    assert result["name"][0] == "John Smith"  # Normal value
    assert pd.isna(result["name"][1])  # "null"
    assert pd.isna(result["name"][2])  # Empty string
    assert pd.isna(result["name"][3])  # Whitespace
    assert pd.isna(result["name"][4])  # "undefined"
    assert pd.isna(result["name"][5])  # "none"

def test_id_column_conversion():
    # ID columns: float to int
    # Create a test configuration
    config = {
        "transformations": {
            "columns": {
                "name": {
                    "required": True,
                    "description": "Full name",
                    "trim": True,
                    "titlecase": True
                }
            }
        }
    }
    
    # Create a test dataframe with ID columns
    data = {
        "hs_object_id": [1234.0, 5678.0, None, 9012.0],
        "customer_record_id": [1001.0, 1002.0, 1003.0, None],
        "normal_float": [10.5, 20.5, 30.5, 40.5],
        "name": ["john smith", "jane doe", "bob johnson", "alice brown"]
    }
    
    df = pd.DataFrame(data)
    
    # print("DEBUG: Original dataframe types:")
    # for col in df.columns:
    #     print(f"  {col}: {df[col].dtype}")
    
    # Transform the data
    transformer = DataTransformer(config)
    result = transformer.transform_dataframe(df)
    
    # print("DEBUG: Transformed dataframe types:")
    # for col in result.columns:
    #     print(f"  {col}: {result[col].dtype}")
    
    # Verify ID column conversions
    for i in range(3):
        if pd.notna(result["hs_object_id"][i]):
            # print(f"hs_object_id[{i}] = {result['hs_object_id'][i]}, type = {type(result['hs_object_id'][i])}")
            assert isinstance(result["hs_object_id"][i], int)
            
    for i in range(3):
        if pd.notna(result["customer_record_id"][i]):
            assert isinstance(result["customer_record_id"][i], int)
    
    # Verify normal float column is unchanged
    for i in range(4):
        if pd.notna(result["normal_float"][i]):
            assert isinstance(result["normal_float"][i], float)
    
    # Verify name transformation still works
    assert result["name"][0] == "John Smith"

def test_actual_config_files():
    """Test that the actual configuration files work with our automatic enhancements"""
    # Paths to config files
    config_pack_path = "config/config-pack"
    contacts_config_dir = Path(config_pack_path) / "contacts"
    companies_config_dir = Path(config_pack_path) / "companies"
    
    # Test both contacts and companies configs
    for config_dir in [contacts_config_dir, companies_config_dir]:
        json_path = config_dir / "transformations.json"
        csv_path = config_dir / "transformations.csv"
        
        # Test CSV to JSON conversion
        output_json = config_dir / "test_output.json"
        csv_to_json_config(str(csv_path), str(output_json))
        
        # Test JSON to CSV conversion
        output_csv = config_dir / "test_output.csv"
        json_to_csv_config(str(json_path), str(output_csv))
        
        # Load JSON config and create transformer
        with open(json_path) as f:
            config = json.load(f)
        
        transformer = DataTransformer(config)
        
        # Create test data that will trigger the automatic enhancements
        data = {
            "email": ["john.doe@gmail.com", "regular@example.com", "invalid@"],
            "phone": ["(555) 123-4567", "555.987.6543", None],
            "company_name" if "company_name" in config["transformations"]["columns"] else "company": 
                ["Acme, INC", "Tech LLC", "Global ltd"],
            "website": ["www.example.com", "https://test.org", "invalid"],
            "first_name" if "first_name" in config["transformations"]["columns"] else "name": 
                ["john", "jane", "robert"],
            "last_name" if "last_name" in config["transformations"]["columns"] else "surname": 
                ["smith", "doe", "johnson"]
        }
        
        # Add config-specific fields
        if "industry" in config["transformations"]["columns"]:
            data["industry"] = ["Tech", "Banking", "Unknown"]
        if "lead_status" in config["transformations"]["columns"]:
            data["lead_status"] = ["New", "In Progress", "Unknown"]
        if "employee_count" in config["transformations"]["columns"]:
            data["employee_count"] = ["100", "-5", "not-a-number"]
        if "created_date" in config["transformations"]["columns"]:
            data["created_date"] = ["2023-01-15", "2023-02-20", "2023-03-25"]
        
        df = pd.DataFrame(data)
        
        # Transform the data
        result = transformer.transform_dataframe(df)
        
        # Verify basic transformations
        company_field = "company_name" if "company_name" in config["transformations"]["columns"] else "company"
        
        # Verify automatic Gmail normalization
        assert result["email"][0] == "johndoe@gmail.com"  # Dots removed
        
        # Verify email validation if configured
        if config["transformations"]["columns"]["email"].get("validate", False):
            assert result["email"][2].startswith("INVALID:")  # Invalid email returns INVALID prefix
        
        # Verify automatic case-insensitive suffix removal
        assert "Inc" not in result[company_field][0]  # INC removed case-insensitively
        assert "LLC" not in result[company_field][1]  # LLC removed
        
        # Verify automatic www removal if validation is enabled
        if config["transformations"]["columns"]["website"].get("validate", False):
            assert not str(result["website"][0]).startswith("www.")  # www removed
            assert result["website"][2].startswith("INVALID:")  # Invalid URL returns INVALID prefix
        
        # Clean up test files
        if output_csv.exists():
            output_csv.unlink()
        if output_json.exists():
            output_json.unlink()
    
    print("Actual configuration files test passed!")

def test_url_validation():
    # After:
    assert transform_methods.url("not-a-valid-url", validate=True) == "INVALID: not-a-valid-url"
    
    # Add more specific test cases
    assert transform_methods.url("example", validate=True) == "INVALID: example"
    assert transform_methods.url("http://a", validate=True) == "INVALID: http://a"

def test_phone_validation():
    """Test phone validation with INVALID prefix."""
    assert transform_methods.phone("123", validate_length=True) == "INVALID: 123"
    assert transform_methods.phone("1234567890", validate_length=True) == "+11234567890"

def test_email_validation():
    """Test email validation with INVALID prefix."""
    assert transform_methods.email("not-an-email", validate=True) == "INVALID: not-an-email"
    assert transform_methods.email("valid@example.com", validate=True) == "valid@example.com"

# Add this to run the test directly if needed
if __name__ == "__main__":
    test_actual_config_files()

