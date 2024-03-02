from etl_practice import log_progress, extract, transform, load_data
import json

# Load the configuration file
with open("./config.json", "r") as config_file:
    config_data = json.load(config_file)

target_file = config_data["target_file"]

log_progress("ETL Job Started") 
log_progress("Extract phase Started") 
extracted_data = extract() 
log_progress("Extract phase Ended") 
log_progress("Transform phase Started") 
transformed_data = transform(extracted_data) 
print("Transformed Data") 
print(transformed_data) 
log_progress("Transform phase Ended") 
log_progress("Load phase Started") 
load_data(target_file, transformed_data) 
log_progress("Load phase Ended") 
log_progress("ETL Job Ended") 