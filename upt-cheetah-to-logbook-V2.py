import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
import re
import time
import numpy as np
import sys

GoogleSheetName = sys.argv[1]
CheetahCrawlerPath = sys.argv[2]
filename = sys.argv[3]

fields = {} #GoogleSheet to Cheetah

with open(filename, 'r') as file:
   for line in file:
       Cheetah_field, GoogleSh_fields = line.strip().split(':')
       for google in GoogleSh_fields.split(','):
           fields[google.strip()] = Cheetah_field

def update_sheets(GoogleSheetName, CheetahCrawlerPath, fields):
    print("updating...\n")
    
    google_fields = list(fields.keys())
    google_run_field = google_fields['run' in google_fields or 'Run' in google_fields]
    cheetah_run_field = fields[google_run_field]
    
    
    scope = ["https://spreadsheets.google.com/feeds",'https://www.googleapis.com/auth/spreadsheets',"https://www.googleapis.com/auth/drive.file","https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name('client_secret.json', scope)
    client = gspread.authorize(creds)

    sheet = client.open(GoogleSheetName).sheet1 #client.open("LOV@EUXFEL_2020").sheet1

    # Extract and print all of the values
    list_of_hashes = sheet.get_all_records()
    google_sheet = pd.DataFrame(list_of_hashes)
    google_runs = [i for i in google_sheet[google_run_field].tolist() if len(str(i)) > 0]
    headers = google_sheet.columns.tolist()
    
    cheetah_df = pd.read_csv(CheetahCrawlerPath, sep=",", header=0)
    cheetah_df = cheetah_df.drop_duplicates() # delete duplicate rows
    cheetah_df.fillna('', inplace=True)
    
    cheetah_runs = [i for i in cheetah_df[cheetah_run_field].tolist() if len(str(i)) > 0]
    
    for run in cheetah_runs:
        #get data from Cheetah crawler
        run_info = cheetah_df[cheetah_df[cheetah_run_field] == run]
        
        if run not in google_runs:
            for google_field in google_fields:
                google_sheet.loc[len(google_runs) + 2, google_field] = run_info[fields[google_field]].item()
            google_runs.append(run)
        else:
            for google_field in google_fields:
                google_sheet.loc[google_sheet[google_run_field] == run, google_field] = run_info[fields[google_field]].item()
    google_sheet.fillna('', inplace=True)
    
    sheet.update([google_sheet.columns.values.tolist()] + google_sheet.values.tolist())
    time.sleep(15)
    
while True:
    update_sheets(GoogleSheetName, CheetahCrawlerPath, fields)
