#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import pickle
import os.path
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
import pandas as pd
import datetime
from pytz import timezone    
from datetime import datetime, timedelta
from datetime import date
from sqlalchemy import create_engine
#from slacknotification_c2c import send_to_slack
import sys
sys.path.append('C:/Users/User/Desktop') #path
import SF_CRED  # import credentials from path
user=SF_CRED.SF_USER
pwd=SF_CRED.SF_PASSWORD
db=SF_CRED.DB
schema=SF_CRED.FIVETRAN_SCHEMA
warehouse=SF_CRED.BI_WH
role=SF_CRED.ROLE

SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']

NDL_ID = '********************************'  #spreadsheet_Id

confirmation_RANGE = 'Sheet5!A:A'    #range

creds = None

if os.path.exists('token.pickle'):
    with open('token.pickle', 'rb') as token:
        creds = pickle.load(token)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
    else:
        flow = InstalledAppFlow.from_client_secrets_file('C:/Users/User/Desktop/google_sheet_python/client_secret.json', SCOPES)
        creds = flow.run_local_server()
    with open('token.pickle', 'wb') as token:
        pickle.dump(creds, token)

service = build('sheets', 'v4', credentials=creds)

sheet = service.spreadsheets()

#bAsic data manipulation
    
confirmation_result = sheet.values().get(spreadsheetId=NDL_ID,range=confirmation_RANGE).execute()
confirmation_values = confirmation_result.get('values', [])
confirmation_headers = confirmation_values.pop(0)
confirmation_data=pd.DataFrame(confirmation_values,columns=confirmation_headers)
#confirmation_data['Campaign'] = 'Confirmation-Control'     
#reminder_data['Campaign'] = 'reminder-Control'      
#reschedule_data['Campaign'] = 'reschedule-Control'   

#Kolkata = timezone('Asia/Kolkata')
#sa_time = date.today()

frames = [confirmation_data]
final_df = pd.concat(frames)
print(final_df)

# writing data into Database

engine =create_engine("snowflake://"+user+":"+pwd+"@am62076.ap-southeast-2/"+db+"/"+schema+"?warehouse="+warehouse+"?role="+role)
connection = engine.connect()
	#Delete_query = """DELETE FROM dim_lead_campaign_channel"""
	#engine.execute(Delete_query)
	# data.to_sql(name='test2',con=connection, if_exists='replace', index=False)
try:
	for k in range(0,len(final_df),10000):
		temp_df = final_df[k:k+9999]
		temp_df.to_sql(name='YOUR_TABLE_NAME',con=connection, if_exists='replace', index=False)

		  
		status = 'success'
except Exception as sqlError:
	status = sqlError
	print(status)

