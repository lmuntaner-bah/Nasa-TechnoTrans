from datetime import datetime
from loguru import logger
import pandas as pd
import os

def save_transformed_data(df, file_name, processed_folder):
    """
    Save the transformed data to a CSV file.
    
    Args:
        df (pandas.DataFrame): The transformed data as a DataFrame.
        file_name (str): The desired name of the output file (without extension).
        processed_folder (str): The path to the folder where the processed data should be saved.
    
    Raises:
        Exception: If an error occurs while saving the DataFrame.
    
    Returns:
        None
    """
    try:
        csv_file_name = f'{file_name}.csv'
        csv_file_path = os.path.join(processed_folder, csv_file_name)
        
        df.to_csv(csv_file_path, index=False)
        
        logger.info(f"Data saved to {csv_file_path}")
    except Exception as e:
        logger.error(f"An error occurred while saving dataframe: {e}")
        raise

def transform_interm_data(df):
    """
    Transforms the intermediate data by renaming columns, dropping unused columns,
    adding new columns, and re-arranging the columns.
    
    Args:
        df (pandas.DataFrame): The input DataFrame containing the intermediate data.
    
    Returns:
        pandas.DataFrame: The transformed DataFrame.
    
    Raises:
        Exception: If an error occurs while transforming the data.
    """
    # Example: new column names list
    new_column_names = [
        "id",
        "ref_id",
        "project_name",
        "project_overview",
        "ref_id_2",
        "category",
        "release_type",
        "notes",
        "download_link",
        "agency",
        "unused_col_1",
        "unused_col_2",
        "unused_col_3"
    ]
    
    # Creating a dictionary: current index to new name
    rename_dict = {i: new_name for i, new_name in enumerate(new_column_names)}
    try:
        # Renaming the columns
        df.rename(columns=rename_dict, inplace=True)
        
        # Replace the empty strings with NaN in the unused columns
        df.replace('', pd.NA, inplace=True)
        
        # Drop the unused columns
        df.drop(columns=['ref_id_2', 'unused_col_1', 'unused_col_2', 'unused_col_3'], inplace=True)
        
        # Add a new column named date_extracted with the current date
        df['date_extracted'] = datetime.now().strftime('%Y-%m-%d')
        
        # Create 'github_repo' column with GitHub links, else NaN
        df['github_repo'] = df['download_link'].apply(lambda x: x if pd.notna(x) and 'github' in x else pd.NA)
        
        # Replace 'download_link' instances containing 'github' with NA
        df['download_link'] = df['download_link'].apply(lambda x: pd.NA if pd.notna(x) and 'github' in x else x)
        
        # Now I want to re-arange the columns
        df = df[['id', 'ref_id', 'agency', 'project_name', 
                'project_overview', 'category', 'release_type', 
                'download_link', 'github_repo', 'notes', 'date_extracted']]
        
        # Drop duplicate rows based on all columns
        df.drop_duplicates(inplace=True)
        
        # Replace NA values with the string 'NA'
        df.fillna('NA', inplace=True)
        
        processed_df = df.copy()
        logger.info("Data transformed successfully.")
        
        return processed_df
    except Exception as e:
        logger.error(f"An error occurred while transforming the data: {e}")