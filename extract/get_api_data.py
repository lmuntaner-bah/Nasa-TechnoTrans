from loguru import logger
import httpx
import json
import time
import os

def _save_data_to_file(data, file_name, directory):
    """
    Save the given data to a file in the specified directory.
    
    Args:
        data (dict): The data to be saved.
        file_name (str): The name of the file.
        directory (str): The directory where the file will be saved.
    
    Returns:
        None
    """
    file_path = os.path.join(directory, file_name)
    
    with open(file_path, 'w') as file:
        json.dump(data, file, indent=4)
    
    logger.info(f"Data saved to {file_path}")

def get_data_from_api(endpoint_url, file_name, directory):
    """
    Fetches data from an API endpoint and saves it to a file.
    
    Args:
        endpoint_url (str): The URL of the API endpoint.
        file_name (str): The name of the file to save the data to.
        directory (str): The directory where the file will be saved.
    
    Returns:
        list: A list containing all the data fetched from the API.
    
    Raises:
        httpx._exceptions.HTTPError: If there is an HTTP error.
        httpx._exceptions.ConnectError: If there is an error connecting to the API.
        httpx._exceptions.TimeoutException: If there is a timeout error.
        httpx._exceptions.RequestError: If there is any other request error.
    """
    all_data = []
    page = 1
    
    while page <= 3:  # Limit to 3 pages
        try:
            # Start measuring time
            start_time = time.time()
            
            logger.info(f"Fetching page {page}...")
            
            # Assuming the API uses 'page' as query parameters for pagination
            response = httpx.get(f"{endpoint_url}&page={page}")
            
            response.raise_for_status()  # Raises an HTTPError if the response status code is 4XX/5XX
            
            data = response.json()  # Returns the json-encoded content of a response, if any.
            
            if not data["results"]:
                break  # Exit loop if no results
            
            all_data.extend(data['results'])  # Aggregate results
            
            # Log time taken for code execution and data extraction
            elapsed_time = time.time() - start_time
            logger.info(f'Code executed and data extracted in {elapsed_time:.2f} seconds for page {page}.')
            
            page += 1
        except httpx._exceptions.HTTPError as errh:
            logger.error(f"Http Error: {errh}")
            break
        except httpx._exceptions.ConnectError as errc:
            logger.error(f"Error Connecting: {errc}")
            break
        except httpx._exceptions.TimeoutException as errt:
            logger.error(f"Timeout Error: {errt}")
            break
        except httpx._exceptions.RequestError as err:
            logger.error(f"Oops: Something Else: {err}")
            break
    
    _save_data_to_file(all_data, file_name, directory)  # Save the aggregated data to a file
    return all_data