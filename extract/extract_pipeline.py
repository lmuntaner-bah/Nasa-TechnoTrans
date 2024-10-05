from get_api_data import get_data_from_api
from dotenv import load_dotenv
from rich import print
import os

load_dotenv()

def main():
    endpoint = f"https://api.nasa.gov/techtransfer/software/data/?engine&api_key={os.getenv('API_KEY')}"
    directory = r"C:\Users\640124\Documents\Code\Dev-Code\Nasa-TechTransfer\data\raw"
    file_name = "nasa_techtransfer_data.json"
    
    data = get_data_from_api(endpoint, file_name, directory)
    
    print(data)

if __name__ == "__main__":
    main()