from loguru import logger
import pandas as pd
import json
import re

def _clean_span_tags(text, pattern_span, pattern_spaces):
    """
    Cleans the given text by removing span tags and replacing multiple spaces with a single space.
    
    Args:
        text (str): The text to be cleaned.
        pattern_span (Pattern): The regular expression pattern for matching span tags.
        pattern_spaces (Pattern): The regular expression pattern for matching multiple spaces.
    
    Returns:
        str: The cleaned text.
    
    Raises:
        Exception: If an error occurs while cleaning the text.
    """
    try:
        # First, remove the span tags
        cleaned_text = pattern_span.sub(r'\1', text)
        
        # Then, replace multiple spaces with a single space
        cleaned_text = pattern_spaces.sub(' ', cleaned_text)
        
        return cleaned_text
    except Exception as e:
        logger.error(f"An error occurred while cleaning the text: {e}")
        raise

def _clean_data(data, pattern_span, pattern_spaces):
    """
    Recursively cleans the given data by removing span tags and extra spaces.
    
    Args:
        data (dict, list, str): The data to be cleaned.
        pattern_span (str): The pattern to match span tags.
        pattern_spaces (str): The pattern to match extra spaces.
    
    Returns:
        dict, list, str: The cleaned data.
    """
    if isinstance(data, dict):
        return {key: _clean_data(value, pattern_span, pattern_spaces) for key, value in data.items()}
    elif isinstance(data, list):
        return [_clean_data(item, pattern_span, pattern_spaces) for item in data]
    elif isinstance(data, str):
        return _clean_span_tags(data, pattern_span, pattern_spaces)
    else:
        return data

def cleaned_interm_data(raw_dir_path):
    """
    Cleans the raw JSON data and transforms it into a pandas DataFrame.
    
    Args:
        raw_dir_path (str): The file path to the raw JSON data.
    
    Returns:
        pandas.DataFrame: The cleaned and transformed data as a DataFrame.
    
    Raises:
        Exception: If an error occurs while cleaning the JSON data.
    """
    try:
        # Compile the regular expression for efficiency
        pattern_span = re.compile(r'<span class=\"highlight\">(.*?)</span>')
        pattern_spaces = re.compile(r'\s+')
        
        # Load the JSON data
        with open(raw_dir_path, 'r') as file:
            data = json.load(file)
        
        # Clean the data
        interm_data = _clean_data(data, pattern_span, pattern_spaces)
        
        # Transform interm_data to a pandas DataFrame
        interm_df = pd.DataFrame(interm_data)
        
        logger.info("JSON data cleaned and transformed to DataFrame.")
        
        return interm_df
    except Exception as e:
        logger.error(f"An error occurred while cleaning the JSON data: {e}")
        raise