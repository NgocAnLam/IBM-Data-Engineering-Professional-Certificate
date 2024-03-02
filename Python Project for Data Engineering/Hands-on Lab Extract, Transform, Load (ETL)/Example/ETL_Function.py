import glob
import json 
import pandas as pd 
import xml.etree.ElementTree as ET 
from datetime import datetime 

### Load the configuration file
with open("config.json", "r") as config_file:
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
    dataframe = pd.DataFrame(columns=["name", "height", "weight"]) 
    tree = ET.parse(file_to_process) 
    root = tree.getroot() 
    for person in root: 
        name = person.find("name").text 
        height = float(person.find("height").text) 
        weight = float(person.find("weight").text) 
        dataframe = pd.concat([dataframe, pd.DataFrame([{
            "name":name, 
            "height":height, 
            "weight":weight
        }])], ignore_index=True) 
    return dataframe 

def extract(): 
    extracted_data = pd.DataFrame(columns=['name','height','weight'])
     
    for csvfile in glob.glob("source/*.csv"): 
        extracted_data = pd.concat([extracted_data, pd.DataFrame(extract_from_csv(csvfile))], ignore_index=True) 
         
    for jsonfile in glob.glob("source/*.json"): 
        extracted_data = pd.concat([extracted_data, pd.DataFrame(extract_from_json(jsonfile))], ignore_index=True) 
     
    for xmlfile in glob.glob("source/*.xml"): 
        extracted_data = pd.concat([extracted_data, pd.DataFrame(extract_from_xml(xmlfile))], ignore_index=True) 
         
    return extracted_data 
### Transform
def transform(data): 
    data['height'] = round(data.height * 0.0254,2) 
    data['weight'] = round(data.weight * 0.45359237,2) 
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


