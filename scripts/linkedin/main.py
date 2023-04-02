#!/usr/bin/env python
# coding: utf-8

# In[0]:

import requests
import pandas as pd
import datetime
from google.cloud import bigquery
from google.oauth2 import service_account
from tqdm import tqdm
from bs4 import BeautifulSoup

# In[1]:

class LinkedIn:
    def __init__(self):
        self.linkedin_df = self.run_proc()

        # self.load_data(self.linkedin_df, 'jobs_details')

    def clean_cols(col_name):
        new_col_name = col_name.lower().replace(' ', '_')
        
        return new_col_name

    def get_date():
        today = datetime.date.today()
        
        yesterday = today - datetime.timedelta(days=1)
        
        yesterday_str = datetime.datetime.strftime(yesterday, '%Y-%m-%d')
            
        return yesterday_str

    def load_data(self, data, table):
        client = bigquery.Client()

        job_config = bigquery.LoadJobConfig()
        job_config.write_disposition = bigquery.job.WriteDisposition.WRITE_APPEND
        # The source format defaults to CSV, so the line below is optional.
        job_config.source_format = bigquery.SourceFormat.CSV
        
        data['dt'] = self.get_date()
                
        project = client.project
        dataset_id = bigquery.DatasetReference(project, 'jobs')
        table_id = dataset_id.table(table)

        job = client.load_table_from_dataframe(data, table_id, job_config=job_config)  # Make an API request.
        job.result()  # Wait for the job to complete.
        
        print(f'Loaded table {table}')
    

    def fix_char(txt):
        # return txt.replace('â\x80\x99', "'").replace('â\x80\x93', '-').replace('&amp;', '&')
        return str(txt).replace('<div class="show-more-less-html__markup show-more-less-html__markup--clamp-after-5">', '').replace('<div>', '').replace('</div>', '')


    def get_jobs(self):
        titles = ['tableau', 'python', 'data%20engineer', 'business%20intelligence', 'analyst', 'business%20analyst', 'data%20analyst']
        remotes = ['', '&f_WT=2']
        
        timing = '86400'
        # timing = '604800'
        
        jobs_df = pd.DataFrame()
        
        for title in titles:
            # for start in tqdm(starts):
            for remote in remotes:
                url = f'https://www.linkedin.com/jobs/search/?f_E=2%2C3%2C4&f_TPR=r{timing}&geoId=103644278&keywords={title}&location=United%20States{remote}'
            
                print(title)
        
                r = requests.get(url, headers={"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.103 Safari/537.36"})
                
                soup = BeautifulSoup(r.text, 'html.parser')
                
                job_posts = soup.select(".base-card")
            
                for job_post in tqdm(job_posts):
                    job_list = [i.text.strip() for i in job_post.select(""".base-search-card__title, 
                                                                        .base-search-card__subtitle > a, 
                                                                        .job-search-card__location,
                                                                        .job-search-card__salary-info""")]
                                                                        
                    job_link = [i['href'] for i in job_post.select(""".base-card__full-link, 
                                                                    .base-search-card__subtitle > a""")]
                                                                        
                    if len(job_list) == 3:
                        job_list.extend([""])
                        job_list.extend(job_link)
                    else:
                        job_list.extend(job_link)
                    
                    job_df = pd.DataFrame(job_list)
                    
                    jobs_df = pd.concat([jobs_df, job_df.T])
        
        jobs_df.columns = ['Job Title', 'Company', 'Location', 'Salary', 'Job Link', 'Company Link']
        
        jobs_df['Job Link'] = jobs_df['Job Link'].str.split('?').str[0]
        jobs_df['Job ID'] = jobs_df['Job Link'].str.split('-').str[-1]
        jobs_df['Salary'] = jobs_df['Salary'].str.replace('\\n\s*', '', regex=True)
                
        jobs_df = jobs_df.reset_index(drop = True)
        
        jobs_df1 = jobs_df.drop_duplicates(subset=['Job ID'], keep='first')
        jobs_df1 = jobs_df1.reset_index(drop = True)
        
        return jobs_df1


    def get_job_details(self, jobs_df1):
        jobs_df1['Description'] = ''
        
        headers = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.103 Safari/537.36"}
        
        for i in tqdm(range(0,len(jobs_df1))):
        # for i in tqdm(range(687,len(jobs_df1))):
            
            url = jobs_df1.loc[i,'Job Link']
            
            if pd.isna(url):
                continue
            else:
                pass
            
            r = requests.get(url, headers=headers)
            
            j=0
            
            while (j < 5) & (jobs_df1.loc[i,'Description'] == ''):
            
                soup = BeautifulSoup(r.text, 'html.parser')
        
                try:
                    job_desc = self.fix_char(soup.find_all("div", {"class": "show-more-less-html__markup"})[0])
                    
                    start = job_desc.find("\n")
                    end = job_desc.rfind("\n")
                    
                    jobs_df1.loc[i,'Description'] = job_desc[start+1:end]
                    
                except:
                    j += 1
                    
                    r = requests.get(url, headers=headers)
        
        return jobs_df1
                                
    def run_proc(self):
        jobs_df1 = self.get_jobs()
        
        jobs_final = self.get_job_details(jobs_df1)
        
        jobs_final.columns = list(map(self.clean_cols,jobs_final.columns))
        
        return jobs_final

# In[2]:

linkedin_df = LinkedIn()

# In[3]:
linkedin_df.linkedin_df

