from fastapi import FastAPI,UploadFile
from pathlib import Path
from pymongo import MongoClient
import xml.etree.ElementTree as ET
from constants import REQUIRED_DATA
import pandas as pd
import os

app = FastAPI()

upload_xml = Path("uploads/xml_files")
upload_excel = "uploads/excel_files"
upload_xml.mkdir(parents=True, exist_ok=True)

CLIENT = MongoClient("mongodb://localhost:27017/")
DB = CLIENT['excel_files']

@app.post('/uploadfile/')
def upload_file(file: UploadFile):
    file_path = upload_xml / file.filename
    with file_path.open("wb") as file_object:
        file_object.write(file.file.read())
    return {"message":"file uploaded"}

@app.post('/convert_xml_to_excel/')
def convert_xml_to_excel():
    file_names = os.listdir(upload_xml)
    if not file_names:
        return {"error":"There is no xml file uploaded"}
    else:
        for file_name in file_names:
            file_path = upload_xml / file_name
            xml_file = open(file_path,"r")
            tree = ET.parse(xml_file)
            root = tree.getroot()
            collected_text = []
            collect_text = False
            LEDGERNAME_DATA = {}
            for element in root.iter():
                if collect_text:
                    if element.tag in REQUIRED_DATA:
                        LEDGERNAME_DATA[element.tag] = element.text
                if element.tag == "LEDGERNAME":
                    LEDGERNAME_DATA = {}
                    LEDGERNAME_DATA[element.tag] = element.text
                    collect_text = True
                if len(LEDGERNAME_DATA.keys()) == len(REQUIRED_DATA):
                    collected_text.append(LEDGERNAME_DATA)
            dataset = pd.DataFrame(collected_text)
            os.makedirs(upload_excel, exist_ok=True)
            excel_file_path = os.path.join(upload_excel, "excel_file.xlsx")
            dataset.to_excel(excel_file_path, index=False)
            return {"message":"Done"}

@app.post('/save_data_in_mongodb/')
def save_data():
    file_names = os.listdir(upload_excel)
    if not file_names:
        return {"error":"There is no excel file uploaded"}
    else:
        for file_name in file_names:
            COLLECTION = DB[file_name]
            excel_data = pd.read_excel(f"uploads/excel_files/{file_name}")
            data_to_insert = excel_data.to_dict(orient="records")
            COLLECTION.insert_many(data_to_insert)
            CLIENT.close()
    return {"message":"Data inserted in mongodb"}
        