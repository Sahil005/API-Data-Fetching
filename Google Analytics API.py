#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import snowflake.connector
import pandas as pd
from apiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime, timedelta
import glob
import re
from sqlalchemy import create_engine
from oauth2client.service_account import ServiceAccountCredentials
import sys
sys.path.append('C:/Users/User/Desktop')
import SF_CRED
user=SF_CRED.SF_USER
pwd=SF_CRED.SF_PASSWORD
db=SF_CRED.DB
schema=SF_CRED.FIVETRAN_SCHEMA
warehouse=SF_CRED.BI_WH
role=SF_CRED.ROLE

SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']
KEY_FILE_LOCATION = 'C:/Users/User/Downloads/llp.json'  # file downloaded from credentials API
VIEW_ID = '186762089'   # the tab ID from GA
page_size = "200000"    # page size for pagination
DIMENSIONS = ["ga:date","ga:adwordsCampaignID","ga:campaign"]  # dimensions that you need to fetch
METRICS = ["ga:adcost","ga:impressions","ga:adClicks"]  # metrics need to fetch


# In[ ]:


def initialize_analyticsreporting():
  """Initializes an Analytics Reporting API V4 service object.

  Returns:
    An authorized Analytics Reporting API V4 service object.
  """
  credentials = ServiceAccountCredentials.from_json_keyfile_name(
      KEY_FILE_LOCATION, SCOPES)

  # Build the service object.
  analytics = build('analyticsreporting', 'v4', credentials=credentials)

  return analytics


# In[ ]:


def get_report(analytics):
  """Queries the Analytics Reporting API V4.

  Args:
    analytics: An authorized Analytics Reporting API V4 service object.
  Returns:
    The Analytics Reporting API V4 response.
  """
  return analytics.reports().batchGet(
      body={
        'reportRequests': [
        {
          'viewId': VIEW_ID,
          'pageSize': page_size,
          'dateRanges': [{'startDate': '2018-08-01', 'endDate': 'today'}],
          'metrics': [{'expression':i} for i in METRICS],
          'dimensions': [{'name':j} for j in DIMENSIONS]
        }]
      }
  ).execute()


# In[ ]:


def convert_to_dataframe(response):
	for report in response.get('reports', []):
		columnHeader = report.get('columnHeader', {})
		dimensionHeaders = columnHeader.get('dimensions', [])
		metricHeaders = [i.get('name',{}) for i in columnHeader.get('metricHeader', {}).get('metricHeaderEntries', [])]
		finalRows = []
		for row in report.get('data', {}).get('rows', []):
			dimensions = row.get('dimensions', [])
			metrics = row.get('metrics', [])[0].get('values', {})
			rowObject = {}
		
			for header, dimension in zip(dimensionHeaders, dimensions):
				rowObject[header] = dimension
			
			for metricHeader, metric in zip(metricHeaders, metrics):
				rowObject[metricHeader] = metric
			finalRows.append(rowObject)
			rowObject['ga:date'] = datetime.strptime(rowObject['ga:date'], '%Y%m%d').strftime('%Y-%m-%d')
	dataFrameFormat = pd.DataFrame(finalRows)
	return dataFrameFormat


# In[ ]:


def main():
    analytics = initialize_analyticsreporting()
    response = get_report(analytics)
    df = convert_to_dataframe(response)
    #df.to_csv('C:/Users/User/Downloads/yu_5.csv', index=False)
    df = df[['ga:date','ga:adwordsCampaignID','ga:campaign','ga:adcost','ga:impressions','ga:adClicks']] 
    df.columns = ['DATE','CAMPAIGN_ID','CAMPAIGN','COST','IMPRESSIONS','CLICKS'] 
    df = df.reset_index(drop = True)
    engine =create_engine("snowflake://"+user+":"+pwd+"@am62076.ap-southeast-2/"+db+"/"+schema+"?warehouse="+warehouse+"?role="+role)
    connection = engine.connect()
    #Delete_query = """DELETE FROM CAMPAIGN_COST"""
    #engine.execute(Delete_query)
	# data.to_sql(name='test2',con=connection, if_exists='replace', index=False)
    try:
        for k in range(0,len(df),10000):
            temp_df = df[k:k+9999]
            temp_df.to_sql(name='YOUR TABLE NAME',con=connection, if_exists='append', index=False)

		  
            status = 'success'
    except Exception as sqlError:
        status = sqlError
        print(status)

    print(df)
# 	print('Excel File Saved!')
if __name__ == '__main__':
    main()

