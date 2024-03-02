import glob
import json 
import pandas as pd 
import xml.etree.ElementTree as ET 
from datetime import datetime 
import warnings
warnings.filterwarnings("ignore")

### Load the configuration file
with open("./config.json", "r") as config_file:
    config_data = json.load(config_file)

### Extract the file paths
log_file = config_data["log_file"]


### Extraction
def extract_from_csv(file_to_process): 
    dataframe = pd.read_csv(file_to_process) 
    return dataframe 

def extract_from_json(file_to_process): 
    dataframe = pd.read_json(file_to_process, lines=True) 
    return dataframe 

def extract_from_xml(file_to_process): 
    dataframe = pd.DataFrame(columns=["car_model", "year_of_manufacture", "price", "fuel"]) 
    tree = ET.parse(file_to_process) 
    root = tree.getroot() 
    for car in root: 
        car_model = car.find("car_model").text 
        year_of_manufacture = int(car.find("year_of_manufacture").text) 
        price = float(car.find("price").text) 
        fuel = car.find("fuel").text
        dataframe = pd.concat([dataframe, pd.DataFrame([{
            "car_model": car_model, 
            "year_of_manufacture": year_of_manufacture, 
            "price": price,
            "fuel": fuel,
        }])], ignore_index=True) 
    return dataframe 

def extract(): 
    extracted_data = pd.DataFrame(columns=["car_model", "year_of_manufacture", "price", "fuel"])
     
    for csvfile in glob.glob("source/*.csv"): 
        extracted_data = pd.concat([extracted_data, pd.DataFrame(extract_from_csv(csvfile))], ignore_index=True) 
         
    for jsonfile in glob.glob("source/*.json"): 
        extracted_data = pd.concat([extracted_data, pd.DataFrame(extract_from_json(jsonfile))], ignore_index=True) 
     
    for xmlfile in glob.glob("source/*.xml"): 
        extracted_data = pd.concat([extracted_data, pd.DataFrame(extract_from_xml(xmlfile))], ignore_index=True) 
         
    return extracted_data 
### Transform
def transform(data): 
    data['price'] = round(data.price, 2) 
    return data 

### Load 
def load_data(target_file, transformed_data): 
    transformed_data.to_csv(target_file) 

### Log
def log_progress(message): 
    timestamp_format = '%Y-%h-%d-%H:%M:%S'
    now = datetime.now()
    timestamp = now.strftime(timestamp_format) 
    with open(log_file,"a") as f: 
        f.write(timestamp + ',' + message + '\n') 


