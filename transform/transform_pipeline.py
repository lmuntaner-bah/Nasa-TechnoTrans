from clean_interm_data import transform_interm_data, save_transformed_data
from clean_api_data import cleaned_interm_data

def main():
    raw_dir_path = r'C:\Users\640124\Documents\Code\Dev-Code\Nasa-TechTransfer\data\raw\nasa_techtransfer_data.json'
    
    processed_dir_path = r'C:\Users\640124\Documents\Code\Dev-Code\Nasa-TechTransfer\data\processed'
    
    filename = 'processed_nasa_techtransfer_data'
    
    interm_df = cleaned_interm_data(raw_dir_path)
    
    transformed_df = transform_interm_data(interm_df)
    
    save_transformed_data(transformed_df, filename, processed_dir_path)

if __name__ == "__main__":
    main()