#!/usr/bin/env python
# coding: utf-8

# **Mode Analytics to Python Connector**

# In[3]:


import requests
from pandas.io.json import json_normalize
import json
import time


# Generate token and secret id using: https://modeanalytics.com/settings/access_tokens

# In[4]:


# #Enter Auth Username and password (token and secret)
# global basicAuthCredentials



# In[5]:


def modeQuery(report_token, query, 
              basicAuthCredentials ,data_source_id=17216, max_attempts=30, sleep_time=10):
    
    values = json.dumps({
      "query": {
      "raw_query": query,
      "data_source_id": data_source_id
      }
    })
    
    headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/hal+json'
    }
    
    #Post query in the given report token
    post_query = requests.post(f'https://modeanalytics.com/api/cars24/reports/{report_token}/queries', auth=basicAuthCredentials,
                               data = values, headers=headers)
    
    query_token = json_normalize(post_query.json())['token'][0]
    
    if(post_query.status_code == 200):
        print('Query created successfully.')
    else:
        print('Exception. Status_code: ', post_query.status_code)
        
    #Run report & get report_run_token
    post_report = requests.post(f'https://modeanalytics.com/api/cars24/reports/{report_token}/runs', auth=basicAuthCredentials)
    print('Report run: started...')
    
    report_run_token = json_normalize(post_report.json())['token'][0]

    status = False
    attempts = 0
    while (status==False)&(attempts<max_attempts):
          try:
                attempts+=1
                #Get status of the report run
                post_report_run = requests.get(f'https://modeanalytics.com/api/cars24/reports/{report_token}/runs/{report_run_token}', auth=basicAuthCredentials)
                
                post_report_run_status = post_report_run.status_code
                completed_at = json_normalize(requests.get(f'https://modeanalytics.com/api/cars24/reports/{report_token}/runs/{report_run_token}', auth=basicAuthCredentials).json())['completed_at'][0]
                
                if(completed_at != None):
                    status = True
                else:
                    status = False
                    print("report_run_status_code: ", post_report_run_status)
                    print("completed_at: ", completed_at)
                    print(f'Attempt: {attempts} report running...')
                    time.sleep(sleep_time)
          
          except Exception as e:
            print(f'''Exception {attempts}: ''',e,"report_run_status_code: ", post_report_run_status)
    print(f'Report run: succesful in {attempts} attempts \n')  
    
    #Get query_run_token
    query_run_token = json_normalize(requests.get(f'https://modeanalytics.com/api/cars24/reports/{report_token}/queries/{query_token}/runs', auth=basicAuthCredentials).json()['_embedded']['query_runs'])['token'][0]
    
    print("report_token: " ,report_token)
    print("query_token: ", query_token)
    print("report_run_token: ", report_run_token)
    print("query_run_token: ", query_run_token)
     
    #Getting query results
    print('\nGetting query_results...')
    df_output = requests.get(f'https://modeanalytics.com/api/cars24/reports/{report_token}/runs/{report_run_token}/query_runs/{query_run_token}/results/content.json', 
                             auth=basicAuthCredentials).json()
    df_output = json_normalize(df_output)

    print("shape: ", df_output.shape)
    
    #Delete query from the report
    print('\nDeleting query...')
    query_delete = requests.delete(f'https://modeanalytics.com/api/cars24/reports/{report_token}/queries/{query_token}',
                                   auth=basicAuthCredentials)
    if(query_delete.status_code == 200):
        print("Results fetched and Query deleted from the report")
    else:
        print("Query delete failed. ", query_delete.status_code, query_delete.json())
    
    return df_output


# In[ ]:





# In[11]:


def data_source_list(basicAuthCredentials):
    data_sources = requests.get('https://modeanalytics.com/api/cars24/data_sources', auth=basicAuthCredentials)
    data_sources = data_sources.json()
    data_sources = json_normalize(data_sources['_embedded']['data_sources'])[['name','id']]
    return(data_sources)


# Create a new report in Mode's UI with atleast one dummy query.
# 
# Copy the report token from the URL (viz, last part of URL after clicking the report name).
# Also enter the data_source_id from the above table.

# In[12]:


# global report_token



# In[ ]:




