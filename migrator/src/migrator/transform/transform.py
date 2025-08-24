from typing import Dict, Any, Optional, Union
import inspect
import json
import re
from pathlib import Path

import pandas as pd

from migrator.utils.logger import get_logger
from migrator.transform.transform_methods import TRANSFORM_REGISTRY

# logger initialization
logger = get_logger()

# TODO(DONE): Further refactor following recent discussion with @reedwi - do away with chain/function delineation.
# TODO(DONE): Transformations are mapped to columns, which will be defined and stored in a dictionary "TRANSFORM_REGISTRY", in a dedicated section of the config. Exact location TBD.
# TODO(DONE): The dictionary will be used to map fields to transformations. 

class DataTransformer:
    def __init__(self, transformation_config: Dict[str, Any]):
        self.config = transformation_config
        
    def _load_mapping(self, column_name: str, mapping_value: Union[str, Dict[str, str]], config_dir: Optional[str] = None) -> Dict[str, str]:
      
        # Load mapping values from JSON string, dictionary, or file.
        #     column_name: Column name for mapping
        #     mapping_value: JSON string, dictionary, or filename for mapping
        #     config_dir: Directory containing mapping files
            
     
        # If it's already a dictionary, return it
        if isinstance(mapping_value, dict):
            return mapping_value
        
        # Try to parse as JSON
        try:
            return json.loads(mapping_value)
        except (json.JSONDecodeError, TypeError):
            # Not valid JSON, try to load from file
            if not config_dir:
                raise ValueError(f"Cannot load mapping for column '{column_name}': config_dir not provided and value is not valid JSON")
                
            mapping_file = Path(config_dir) / f"mappings_{column_name}.json"
            if not mapping_file.exists():
                logger.error(f"Mapping file for column '{column_name}' not found at: {mapping_file}")
                raise ValueError(f"Mapping file for column '{column_name}' not found at: {mapping_file}")
                
            with open(mapping_file) as f:
                try:
                    return json.load(f)
                except json.JSONDecodeError as e:
                    logger.error(f"Invalid JSON in mapping file for column '{column_name}': {e}")
                    raise ValueError(f"Invalid JSON in mapping file for column '{column_name}': {e}")
    
    def _apply_mapping(self, value: Any, mapping: Dict[str, str], case_sensitive: bool = False) -> Any:
        """Apply mapping transformation to a value."""
        if pd.isna(value):
            return value
            
        # Handle non-string values
        if not isinstance(value, str):
            try:
                str_value = str(value)
            except:
                return value
        else:
            str_value = value
        lookup_value = str_value if case_sensitive else str_value.lower()
        
        if not case_sensitive:
            # Convert keys to lowercase for case-insensitive lookup
            mapping = {k.lower() if k != '*' else k: v for k, v in mapping.items()}
            
        # Return mapped value or default (*) or original
        return mapping.get(lookup_value, mapping.get('*', value))
    
    def _apply_column_transforms(self, series: pd.Series, column_name: str, config: Dict[str, Any], config_dir: Optional[str] = None) -> pd.Series:
        """
        Apply transformations to a single column.
        
        Args:
            series: Column data to transform
            column_name: Column name
            config: Transformation configuration for this column
            config_dir: Directory containing config files
            
        Returns:
            Transformed column data
        """
        result = series.copy()
        
        # Apply transformations in the order defined in TRANSFORM_REGISTRY
        for transform_name, transform_func in TRANSFORM_REGISTRY.items():
            # Check if this transformation should be applied
            should_apply = (
                column_name.lower() == transform_name.lower() or
                (transform_name in config and config[transform_name])
            )
            
            if not should_apply:
                continue
                
            # Get the parameters that the transformation function accepts
            sig = inspect.signature(transform_func)
            valid_params = {
                param.name for param in sig.parameters.values() 
                if param.kind != inspect.Parameter.VAR_KEYWORD
            }
            
            # Filter kwargs to only include parameters that the function accepts
            kwargs = {k: v for k, v in config.items() 
                    if k not in ['required', 'description', 'mapping_values', 'case_sensitive'] 
                    and (k in valid_params or 'kwargs' in sig.parameters)}
            
            # Apply the transformation
            try:
                result = result.apply(lambda x: transform_func(x, **kwargs))
            except Exception as e:
                logger.error(f"Error applying {transform_name} to column {column_name}: {str(e)}")
                # If this is a required column and transformation, re-raise the exception
                if config.get('required', False):
                    raise ValueError(f"Failed to apply required transformation {transform_name} to column {column_name}: {str(e)}")
        
        # Apply mapping if specified (after all transformations)
        if 'mapping_values' in config and pd.notna(config['mapping_values']):
            try:
                mapping = self._load_mapping(column_name, config['mapping_values'], config_dir)
                case_sensitive = config.get('case_sensitive', False)
                result = result.apply(lambda x: self._apply_mapping(x, mapping, case_sensitive))
            except Exception as e:
                logger.warning(f"Error applying mapping to column {column_name}: {str(e)}")
                if config.get('required', False):
                    logger.error(f"Failed to apply required mapping to column {column_name}: {str(e)}")
                    raise ValueError(f"Failed to apply required mapping to column {column_name}: {str(e)}")
            
        return result
    
    def transform_dataframe(self, df: pd.DataFrame, config_dir: Optional[str] = None) -> pd.DataFrame:
        # df: Dataframe to transform
        # config_dir: Directory containing config files
        
        result = df.copy()
        
        # Create a mapping of lowercase column names to actual column names
        column_map = {col.lower(): col for col in result.columns}
        
        # Convert falsey values to None
        result = self._convert_falsey_values(result)
        
        # Convert ID columns from float to int
        
        result = self._convert_id_columns(result)
        
        # Get column configurations
        column_configs = self.config.get('transformations', {}).get('columns', {})
        
        # Apply transformations to each configured column
        for column_name, column_config in column_configs.items():
            # Check for column name variations (case-insensitive)
            column_lower = column_name.lower()
            
            # Handle column name aliases
            if column_lower == 'company_name':
                # Look for 'company_name', 'company', or 'name'
               self._handle_company_name_column(result, column_name, column_config, column_map) 
                                    
            elif column_lower in column_map:
                # Column exists with different case
                actual_col = column_map[column_lower]
                if actual_col != column_name:
                    
                    result[column_name] = result[actual_col].copy()
                    
            elif column_config.get('required', False):
                # Required column not found
                logger.error(f"Required column '{column_name}' not found in dataframe")
                raise ValueError(f"Required column '{column_name}' not found in dataframe")
                
            # Apply transformations if column exists or was created
            if column_name in result.columns:
                result[column_name] = self._apply_column_transforms(
                    result[column_name], 
                    column_name, 
                    column_config,
                    config_dir
                )
        
        return result

    def _convert_falsey_values(self, df: pd.DataFrame) -> pd.DataFrame:
    #Created to account for concerns raised in comment here: https://github.com/Aptitude-8/acrisure-migrator/pull/29#issuecomment-2674715849
        result = df.copy()
        
        # Define falsey values
        falsey_strings = ['null', 'none', 'nan', 'undefined', '']
        
        # Apply to all string columns
        for column in result.columns:
            # Skip non-string columns
            if result[column].dtype != 'object':
                continue
            
            # Convert falsey values to None
            result[column] = result[column].apply(
                lambda x: None if pd.isna(x) or (isinstance(x, str) and x.strip().lower() in falsey_strings) else x
            )
        
        return result

    def _convert_id_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        # Created to account for concerns raised in comment here: https://github.com/Aptitude-8/acrisure-migrator/pull/29#issuecomment-2674715849
        """Convert ID columns from float to int."""
        result = df.copy()
        
        # Identify ID columns using compiled patterns for efficiency
        id_patterns = [re.compile(p, re.IGNORECASE) for p in ['hs_object_id', 'record_id', '_id$']]
        id_columns = [col for col in result.columns if any(p.search(str(col)) for p in id_patterns)]
        
        if id_columns:
            logger.info(f"Identified ID columns: {id_columns}")
        else:
            logger.info("No ID columns identified")
        
        # Convert float ID columns to int
        for column in id_columns:
            # Skip if column is not numeric or already integer
            if not pd.api.types.is_numeric_dtype(result[column]) or pd.api.types.is_integer_dtype(result[column]):
                continue
            
            # Check if all non-null values are integers
            non_null_values = result[column].dropna()
            if len(non_null_values) == 0 or not all(float(x).is_integer() for x in non_null_values):
                # Column contains non-integer values, skip conversion
                logger.warning(f"Column {column} contains non-integer values, skipping conversion")
                continue
            
            # Convert to object type first to handle NaN values properly
            result[column] = result[column].astype('object')
            
            # Convert integer-like values to Python int, preserving NaN values
            result[column] = result[column].apply(
                lambda x: int(x) if pd.notna(x) else x
            )
        
        return result

    def _handle_company_name_column(self, df: pd.DataFrame, column_name: str, column_config: Dict[str, str], column_map: Dict[str, str]) -> None:
        # Handle the company_name column, which may be aliased as company, name, or company_name, etc.
        possible_columns = ['company_name', 'company', 'name']
        for possible_col in possible_columns:
            if possible_col.lower() in column_map:
                actual_col = column_map[possible_col.lower()]
                df[column_name] = df[actual_col].copy()
                # Hit if we find a match
                return  
        
        if column_config.get('required', False):
            logger.error(f"Required column '{column_name}' not found in dataframe. "
                            f"Looked for variations: {possible_columns}")
            raise ValueError(f"Required column '{column_name}' not found in dataframe. "
                            f"Looked for variations: {possible_columns}")

    def transform_file(self, input_path: Union[str, Path, None] = None, 
                      output_path: Union[str, Path, None] = None,
                      input_stream = None, output_stream = None,
                      config_dir: Optional[str] = None) -> Optional[pd.DataFrame]:
        try:
            # 1. Load data
            if input_stream is not None:
                df = pd.read_csv(input_stream)
            elif input_path:
                input_path = Path(input_path)
                if not input_path.exists():
                    logger.error(f"Input file not found: {input_path}")
                    return None
                df = pd.read_csv(input_path)
            else:
                logger.error("No input source provided")
                return None
            
            # 2. Transform data
            result = self.transform_dataframe(df, config_dir)
            
            # 3. Save result
            if output_stream is not None:
                result.to_csv(output_stream, index=False)
            elif output_path:
                output_path = Path(output_path)
                output_path.parent.mkdir(parents=True, exist_ok=True)
                result.to_csv(output_path, index=False)
                logger.info(f"Transformation complete. Output saved to {output_path}")
            else:
                logger.debug("No output destination provided, returning DataFrame only")
            
            return result
            
        except FileNotFoundError as e:
            logger.error(f"File not found: {e}")
            return None
        except pd.errors.ParserError as e:
            logger.error(f"Error parsing CSV data: {e}")
            return None
        except ValueError as e:
            logger.error(f"Validation error during transformation: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error during transformation process: {e}")
            return None

