# List of methods for transforming data

from typing import Any
import re
from datetime import datetime
from urllib.parse import urlparse

import pandas as pd
import phonenumbers

from migrator.utils.logger import get_logger


logger = get_logger()
# Simple transformations
def trim(value: Any) -> Any:
    """Trim whitespace from the beginning and end of a string."""
    if pd.isna(value) or not isinstance(value, str):
        return value
    return value.strip()

def lowercase(value: str) -> str:
    """Convert a string to lowercase."""
    if pd.isna(value) or not isinstance(value, str):
        return value
    return str(value).lower()

def uppercase(value: str) -> str:
    """Convert string to uppercase."""
    if pd.isna(value) or not isinstance(value, str):
        return value
    return value.upper()

def titlecase(value: str) -> str:
    """Convert a string to title case."""
    if pd.isna(value) or not isinstance(value, str):
        return value
    return str(value).title()

def phone(value: Any, **kwargs) -> Any:
    """
    Format phone number with country code.
    
    Args:
        value: Phone number to transform
        **kwargs: Additional parameters
            - country_code: Country code to use (default: '')
            - default_region: Default region for parsing (default: 'US')
            - validate_length: Whether to validate length (default: True)
            
    Returns:
        Formatted phone number, or "INVALID: {original_value}" if validation fails
    """

    if pd.isna(value) or value is None:
        return None
        
    # Get parameters from kwargs
    country_code = kwargs.get('country_code', '')
    default_region = kwargs.get('default_region', 'US')
    validate_length = kwargs.get('validate_length', True)
    
    # Remove all non-numeric characters for basic processing
    digits = re.sub(r'\D', '', str(value))
    
    # Handle different formats
    if validate_length and len(digits) < 10:  # Too short
        logger.warning(f"Phone number too short: {value}")
        return f"INVALID: {value}"
    
    # Fast path for common case: US numbers with country code 1
    if country_code == '1' and len(digits) == 10:
        return f"+{country_code}{digits}"
    
    # Try to use phonenumbers library for formatting
    if phonenumbers:
        try:
            # Try parsing strategies in order of preference
            parsing_attempts = [
                lambda: phonenumbers.parse(str(value), default_region),
                lambda: phonenumbers.parse(digits, default_region),
                lambda: phonenumbers.parse(f"+{country_code}{digits}", None) if country_code else None
            ]
            
            phone_obj = None
            for parse_attempt in parsing_attempts:
                try:
                    result = parse_attempt()
                    if result:
                        phone_obj = result
                        break
                except phonenumbers.NumberParseException:
                    continue
            
            if phone_obj and phonenumbers.is_valid_number(phone_obj):
                return phonenumbers.format_number(phone_obj, phonenumbers.PhoneNumberFormat.E164)
            
            logger.warning(f"Could not parse phone number with phonenumbers library: {value}")
        except Exception as e:
            logger.warning(f"Error using phonenumbers library: {str(e)}")
    
    # Fallback to simple formatting
    if country_code:
        if len(digits) == 10:
            return f"+{country_code}{digits}"
        elif digits.startswith(country_code):
            return f"+{digits}"
        else:
            return f"+{country_code}{digits}"
    
    return digits

def email(value: Any, **kwargs) -> Any:
    """
    Transform an email address.
    
    Args:
        value: Email address to transform
        **kwargs: Additional parameters
            - validate: Validate email format
            
    Returns:
        Transformed email address
    """
    if pd.isna(value) or not isinstance(value, str):
        return None
    
    # Convert to string and lowercase
    email_str = str(value).strip().lower()
    
    # Automatically normalize Gmail addresses by removing dots
    if '@gmail.com' in email_str:
        username, domain = email_str.split('@', 1)
        username = username.replace('.', '')
        email_str = f"{username}@{domain}"
    
    # Validate email format
    if kwargs.get('validate', False):
        email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        if not re.match(email_pattern, email_str):
            logger.warning(f"Invalid email format: {value}")
            return f"INVALID: {value}"
    
    return email_str

def date(value: Any, **kwargs) -> str:
    """
    Standardize date format.
    
    Args:
        value: Date to transform
        **kwargs: Additional parameters
            - date_format: Output date format (default: '%Y-%m-%d')
            
    Returns:
        Formatted date string, or "INVALID: {original_value}" if parsing fails
    """
    if pd.isna(value) or value is None:
        return None
        
    # Get format from kwargs if provided
    date_format = kwargs.get('date_format', '%Y-%m-%d')
    
    try:
        # If it's already a datetime object
        if isinstance(value, datetime):
            return value.strftime(date_format)
            
        # Try common date formats
        for fmt in ['%Y-%m-%d', '%m/%d/%Y', '%d/%m/%Y', '%Y/%m/%d', '%m-%d-%Y', '%d-%m-%Y']:
            try:
                dt = datetime.strptime(str(value), fmt)
                return dt.strftime(date_format)
            except ValueError:
                continue
                
        # If all else fails, try pandas to_datetime
        dt = pd.to_datetime(value)
        return dt.strftime(date_format)
    except Exception as e:
        logger.warning(f"Error formatting date {value}: {str(e)}")
        return f"INVALID: {value}"

def company(value: Any, **kwargs) -> Any:
    """Clean and standardize company names, automatically removing common suffixes."""
    if pd.isna(value) or not isinstance(value, str):
        return value
    
    # No need to strip here if trim is applied first
    result = value
    
    # Default suffixes to remove if not specified
    default_suffixes = "Inc,LLC,Ltd,Corp,Corporation,Company,Co,Limited"
    
    # Remove suffixes if specified or use defaults
    remove_suffixes = kwargs.get('remove_suffixes', default_suffixes)
    case_sensitive = kwargs.get('case_sensitive', False)
    
    if remove_suffixes:
        # Clean up the suffixes by removing periods
        suffixes = [s.strip().replace('.', '') for s in remove_suffixes.split(',')]
        for suffix in suffixes:
            pattern = r'[\s,\.]*' + re.escape(suffix) + r'[\s,\.]*$'
            flags = 0 if case_sensitive else re.IGNORECASE
            result = re.sub(pattern, '', result, flags=flags)
    
    # Clean up any trailing commas, periods, and whitespace
    result = re.sub(r'[\s,\.]+$', '', result)
    
    return result

def url(value: Any, **kwargs) -> Any:
    """
    Clean and standardize URL, automatically removing www prefix.
    
    Args:
        value: URL to transform
        **kwargs: Additional parameters
            - validate: Validate URL format (default: False)
            - remove_www: Remove www prefix (default: True)
            
    Returns:
        Transformed URL, or "INVALID: {original_url}" if validation fails
    """
    if pd.isna(value) or not isinstance(value, str):
        return value
    
    # No need to strip here if trim is applied first
    # Just lowercase the value
    result = value.lower()
    
    # Skip empty strings - this should be handled by _preprocess_dataframe
    if not result:
        return None
    
    # Ensure URL starts with http:// or https://
    if result and not result.startswith(('http://', 'https://')):
        result = 'https://' + result
    
    # Validate URL if requested
    if kwargs.get('validate', False):
        try:
            parsed = urlparse(result)
            # Check for valid domain structure (must have a netloc with at least one dot)
            if not parsed.netloc or '.' not in parsed.netloc:
                return f"INVALID: {value}"  # Invalid domain structure
                
            # Check for common TLDs to further validate
            tld = parsed.netloc.split('.')[-1]
            if len(tld) < 2:  # TLDs are at least 2 characters
                return f"INVALID: {value}"  # Invalid TLD
                
            # Automatically remove www prefix (no parameter needed)
            domain = parsed.netloc.replace('www.', '')
            result = f"{parsed.scheme}://{domain}{parsed.path}"
            if parsed.query:
                result += f"?{parsed.query}"
            if parsed.fragment:
                result += f"#{parsed.fragment}"
        except Exception as e:
            logger.warning(f"Error validating URL {value}: {str(e)}")
            return f"INVALID: {value}"  # Exception during validation
    elif kwargs.get('remove_www', True):
        # If not validating but still want to remove www
        try:
            parsed = urlparse(result)
            domain = parsed.netloc.replace('www.', '')
            result = f"{parsed.scheme}://{domain}{parsed.path}"
            if parsed.query:
                result += f"?{parsed.query}"
            if parsed.fragment:
                result += f"#{parsed.fragment}"
        except Exception:
            logger.warning(f"Error removing www from URL {value}: Returning original value")
            pass
    
    return result

def number(value: Any, **kwargs) -> Any:
    """
    Standardize numeric values.
    
    Args:
        value: Numeric value to transform
        **kwargs: Additional parameters
            - min_value: Minimum allowed value
            - max_value: Maximum allowed value
            - default_value: Default value if conversion fails
            
    Returns:
        Transformed numeric value, original value, or default value
    """
    if pd.isna(value) or value is None:
        return None
        
    # Get parameters from kwargs
    min_value = kwargs.get('min_value')
    max_value = kwargs.get('max_value')
    default = kwargs.get('default_value')
    
    try:
        # Enhanced string-to-number conversion from standardize_number
        if isinstance(value, str):
            value = float(re.sub(r'[^\d.-]', '', value))
        else:
            value = float(value)
        
        # Check minimum value if specified
        if min_value is not None and value < float(min_value):
            return float(min_value) if default is None else float(default)
            
        # Check maximum value if specified (from standardize_number)
        if max_value is not None and value > float(max_value):
            return float(max_value)  # Return max_value instead of default
            
        return value
    except Exception as e:
        logger.warning(f"Error converting to number {value}: {str(e)}")
        return default if default is not None else value

def remove_titles(value: Any, **kwargs) -> Any:
    """
    Remove common name titles from the beginning of a string.
    
    Args:
        value: String to transform
        **kwargs: Additional parameters
            - titles: List of titles to remove (default: common English titles)
            - case_insensitive: Whether to match case-insensitively (default: False)
            
    Returns:
        String with titles removed
    """
    if pd.isna(value) or not isinstance(value, str):
        return value
    
    # Get custom titles if provided
    default_titles = ['Mr.', 'Mrs.', 'Ms.', 'Miss', 'Dr.', 'Prof.', 'Rev.', 'Hon.', 'Sir', 'Madam']
    titles = kwargs.get('titles', default_titles)
    
    result = value.strip()
    
    # Use case-insensitive matching if requested (from first remove_titles)
    if kwargs.get('case_insensitive', False):
        for title in titles:
            result = re.sub(f"^{title}\\s+", "", result, flags=re.IGNORECASE)
    else:
        # Original implementation
        for title in titles:
            if result.startswith(title + ' '):
                result = result[len(title):].strip()
    
    return result


# Global registry of transformation functions
# In cases where there are multiple transformations to be applied, the order of the transformations will be dictated
# by the ordering within the dictionary.
TRANSFORM_REGISTRY = {
    # Basic text transformations 
    'trim': trim,
    'lowercase': lowercase,
    'uppercase': uppercase,
    'titlecase': titlecase,
    
    # Content-specific transformations
    'remove_titles': remove_titles,
    'company': company,
    
    # Format-specific transformations 
    'email': email,
    'phone': phone,
    'date': date,
    'url': url,
    'number': number
}

# List of all available transformation methods
__all__ = list(TRANSFORM_REGISTRY.keys()) 



