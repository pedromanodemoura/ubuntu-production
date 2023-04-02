#!/usr/bin/env python
# coding: utf-8

# In[0]:

import requests
import pandas as pd
import datetime
from google.cloud import bigquery
from google.oauth2 import service_account

# In[1]:

class IMDb:
    def __init__(self, date_list = None):
        if date_list is None:
            self.yesterday = self.get_date()
            self.imdb = self.get_movies_df()
            # self.load_data(self.imdb, 'imdb')
        else:
            for day in date_list:
                print(day)
                self.yesterday = day
                self.imdb = self.get_movies_df()
                # self.load_data(self.imdb, 'imdb')

    def next_90_days(self):

        File_object = open(r'C:\Users\c20460\Desktop\Projects\Movies\IMDb\API\next_day.txt',"r+")

        start_day = File_object.read()

        numdays = 90
        date_list = [datetime.datetime.strptime(start_day,'%Y-%m-%d') + datetime.timedelta(days=-x) for x in range(numdays)]
        File_object.close()

        return date_list

    def get_date(self):
        today = datetime.date.today()
        yesterday = today - datetime.timedelta(days=1)
        yesterday_str = datetime.datetime.strftime(yesterday,'%Y-%m-%d')
        
        print(yesterday_str)

        return yesterday_str

    def get_movies_df(self):
        url = 'https://imdb-api.com/API/AdvancedSearch/k_91apvlh2?release_date=' + self.yesterday + ',' + self.yesterday + '&num_votes=50,&count=250&sort=moviemeter,desc'
        
        payload = {}
        headers= {}
        
        response = requests.request("GET", url, headers=headers, data = payload)
        
        df = pd.json_normalize(response.json()['results'], max_level=0)

        df['dt'] = self.yesterday

        return df

    def load_data(self, data, table):
        client = bigquery.Client()

        job_config = bigquery.LoadJobConfig()
        job_config.write_disposition = bigquery.job.WriteDisposition.WRITE_APPEND
        # The source format defaults to CSV, so the line below is optional.
        job_config.source_format = bigquery.SourceFormat.CSV
        
        project = client.project
        dataset_id = bigquery.DatasetReference(project, 'movies')
        table_id = dataset_id.table(table)

        job = client.load_table_from_dataframe(data, table_id, job_config=job_config)  # Make an API request.
        job.result()  # Wait for the job to complete.

        print(f'Loaded table {table}')

# In[2]:

imdb_res = IMDb()
imdb_res.imdb

# In[3]

imdb_res = IMDb(['2023-03-01', '2023-02-01'])
imdb_res.imdb

# %%

# =============================================================================
##### Gather title details
# titles_df = pd.DataFrame()
# sites_df = pd.DataFrame()
# 
# for title in titles:
#     day_str = datetime.datetime.strftime(day,'%Y-%m-%d')
#     print(day_str)
#     url = 'https://imdb-api.com/API/Title/k_91apvlh2/' + title
#      
#     payload = {}
#     headers= {}
#      
#     response = requests.request("GET", url, headers=headers, data = payload)
#      
#     df = pd.json_normalize(response.json()['results'], max_level=0)
#     
#     titles_df = titles_df.append(df)
# 
# 
#     url = 'https://imdb-api.com/API/ExternalSites/k_91apvlh2/' + title
# 
#     response = requests.request("GET", url, headers=headers, data = payload)
#      
#     df = pd.json_normalize(response.json()['results'], max_level=0)
#     
#     sites_df = sites_df.append(df)
# 
# =============================================================================