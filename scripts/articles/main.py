#!/usr/bin/env python
# coding: utf-8

# In[7]:

import warnings
warnings.filterwarnings("ignore")

import requests
import math
import pandas as pd
import time
from google.cloud import bigquery
from bs4 import BeautifulSoup
import re
import datetime
#import feedparser
from abc import ABC, abstractmethod


# In[6]:

class Articles(ABC):
    def __init__(self):
        # self.website = website
        pass

    # @abstractmethod
    def get_request(self, url, type = 'get'):
        if type == 'get':
            r = requests.get(url, headers={"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.103 Safari/537.36"})
        else:
            r = requests.post(url, headers={"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.103 Safari/537.36"})

        return r
    
    def load_data(self, data, table):
        client = bigquery.Client()

        job_config = bigquery.LoadJobConfig()
        job_config.write_disposition = bigquery.job.WriteDisposition.WRITE_APPEND
        # The source format defaults to CSV, so the line below is optional.
        job_config.source_format = bigquery.SourceFormat.CSV

        project = client.project
        dataset_id = bigquery.DatasetReference(project, 'articles')
        table_id = dataset_id.table(table)

        job = client.load_table_from_dataframe(data, table_id, job_config=job_config)  # Make an API request.
        job.result()  # Wait for the job to complete.

        print(f'Loaded table {table}')
    
    @abstractmethod
    def get_url(self):
        pass

    @abstractmethod
    def get_df(self):
        pass
    
    
class StitchFix(Articles):
    def __init__(self, page):
        # super().__init__("Stitch Fix")
        self.page = page
        self.url = self.get_url()
        self.r = super().get_request(self.url)
        self.soup = BeautifulSoup(self.r.content.decode('utf-8'), 'html.parser')
        self.num_pages = self.get_pages()
        self.article = self.get_df()
        # super().load_data(self.article, 'data_articles')

    def get_pages(self):
        num_pages = int(re.search('Page: 1 of ([0-9]*)',self.soup.select(".page_number")[0].text).group(1))

        return num_pages

    def get_url(self):
        if self.page == 1:
            url = 'https://multithreaded.stitchfix.com/blog/'
        elif self.page > 1:
            url = f'https://multithreaded.stitchfix.com/blog/page/{self.page}/'
        else:
            raise Exception('Page number must be above 0')
        
        return url
    
    def get_df(self):
        articles = self.soup.select(".post-listing")
        
        article = pd.DataFrame()

        article['title'] = [i.select(".h4")[0].text.replace('\n', '') for i in articles]
        article['link'] = ["https://multithreaded.stitchfix.com" + i.select(".h4 > a")[0]['href'] for i in articles]
        article['summary'] = [i.select("p")[0].text if len(i.select("p"))>0 else i.select(".post-byline")[0].next_sibling.strip() for i in articles]
        article['author'] = [i.select(".post-author")[0].text.strip() for i in articles]
        article['pub_date'] = [datetime.datetime.strptime(i.select("time")[0].text, '%B %d, %Y') for i in articles]
        article['website'] = 'Stitch Fix'

        return article

class AnalyticsVidhya(Articles):
    def __init__(self, page):
        # super().__init__("Analytics Vidhya")
        self.page = page
        self.url = self.get_url()
        self.r = super().get_request(self.url)
        self.soup = BeautifulSoup(self.r.content.decode('utf-8'), 'html.parser')
        self.num_pages = self.get_pages()
        self.article = self.get_df()
        # super().load_data(self.article, 'data_articles')

    def get_pages(self):
        num_pages = int(self.soup.select(".page-numbers")[3].text)

        return num_pages

    def get_url(self):
        if self.page >= 1:
            url = f'https://www.analyticsvidhya.com/blog-archive/{self.page}/'
        else:
            raise Exception('Page number must be above 0')
        
        return url
    
    def get_df(self):
        articles = self.soup.select(".list-card-content")
        
        article = pd.DataFrame()

        article['title'] = [i.select("h4")[0].text for i in articles]
        article['link'] = [i.select("a")[0]['href'] for i in articles]
        article['author'] = [i.select("h6 > a")[0].text for i in articles]
        article['pub_date'] = [datetime.datetime.strptime(i.select("h6 > a")[0].next_sibling[2::], '%B %d, %Y') for i in articles]
        article['website'] = 'Analytics Vidhya'

        return article    

class CleverProgrammer(Articles):
    def __init__(self, page):
        # super().__init__("Clever Programmer")
        self.page = page
        self.url = self.get_url()
        self.r = super().get_request(self.url)
        self.soup = BeautifulSoup(self.r.content.decode('utf-8'), 'html.parser')
        self.num_pages = int(self.get_pages())
        self.article = self.get_df()
        # super().load_data(self.article, 'data_articles')

    def get_pages(self):
        num_pages = self.soup.select(".page-numbers")[-2].text

        return num_pages

    def get_url(self):
        if self.page >= 1:
            url = f'https://thecleverprogrammer.com/machine-learning/page/{self.page}/'
        else:
            raise Exception('Page number must be above 0')
        
        return url
    
    def get_df(self):
        articles = self.soup.select(".entries")[0]
        
        article = pd.DataFrame()

        article['title'] = [i.select(".entry-title")[0].text.strip() for i in articles]
        article['link'] = [i.select(".entry-title > a")[0]['href'] for i in articles]
        article['author'] = [i.select(".meta-author")[0].text for i in articles]
        article['pub_date'] = [datetime.datetime.strptime(i.select(".meta-date")[0].text, '%B %d, %Y') for i in articles]
        article['website'] = 'Clever Programmer'

        return article  

class freeCodeCamp(Articles):
    def __init__(self, page):
        # super().__init__("freeCodeCamp")
        self.page = page
        self.url = self.get_url()
        self.r = super().get_request(self.url)
        self.soup = BeautifulSoup(self.r.content.decode('utf-8'), 'html.parser')
        self.num_pages = 400
        self.article = self.get_df()
        # super().load_data(self.article, 'data_articles')

    def get_pages(self):
        num_pages = re.search('Page: 1 of ([0-9]*)',self.soup.select(".page_number")[0].text).group(1)

        return num_pages

    def get_url(self):
        if self.page == 1:
            url = 'https://www.freecodecamp.org/news/'
        elif self.page > 1:
            url = f'https://www.freecodecamp.org/news/{self.page-1}/'
        else:
            raise Exception('Page number must be above 0')
        
        return url
    
    def get_df(self):
        article = pd.DataFrame()

        article['title'] = [i.text.strip() for i in self.soup.select(".post-card-title")]
        article['link'] = ['https://www.freecodecamp.org' + i['href'] for i in self.soup.select(".post-card-title > a")]
        article['author'] = [i.select('.meta-item')[0].text.strip() if len(i.select('.meta-item')) > 0 else "No Author" for i in self.soup.select('.post-card-meta')]
        article['pub_date'] = [datetime.datetime.strptime(i.select('time')[0]['datetime'][:24], '%a %b %d %Y %H:%M:%S') for i in self.soup.select('.post-card-meta')]
        article['website'] = 'freeCodeCamp'

        return article

class RealPython(Articles):
    def __init__(self, page):
        # super().__init__("Real Python")
        self.page = page
        self.url = self.get_url()
        self.r = super().get_request(self.url)
        self.soup = self.r.json()
        self.num_pages = self.get_pages()
        self.article = self.get_df()
        # super().load_data(self.article, 'data_articles')

    def get_pages(self):
        num_pages = math.ceil(self.r.json()['total']/20)

        return num_pages

    def get_url(self):
        if self.page >= 1:
            url = f'https://realpython.com/search/api/v2/?kind=article&order=newest&continue_after={(self.page-1)*20}'
        else:
            raise Exception('Page number must be above 0')
        
        return url
    
    def get_df(self):
        articles = pd.json_normalize(self.soup['results'])

        article = articles[['title', 'url', 'description', 'pub_date']]
        article.columns = ['title', 'link', 'summary', 'pub_date']
        article['link'] = 'https://realpython.com' + article['link']
        article['pub_date'] = [datetime.datetime.strptime(i[:-6], "%Y-%m-%dT%H:%M:%S").date() if i != None else i for i in article['pub_date']]
        article['website'] = 'Real Python'

        return article

class devto(Articles):
    def __init__(self, page):
        # super().__init__("dev.to")
        self.page = page
        self.url = self.get_url()
        self.r = super().get_request(self.url)
        self.soup = BeautifulSoup(self.r.content.decode('utf-8'), 'html.parser')
        self.num_pages = 1000
        self.article = self.get_df()
        # super().load_data(self.article, 'data_articles')

    def get_pages(self):
        num_pages = re.search('Page: 1 of ([0-9]*)',self.soup.select(".page_number")[0].text).group(1)

        return num_pages

    def get_url(self):
        if self.page >= 1:
            url = f'https://dev.to/feed?page={self.page}'
        else:
            raise Exception('Page number must be above 0')
        
        return url
    
    def get_df(self):        
        article = pd.DataFrame()

        article['title'] = [i.text for i in self.soup.select('title')[1::]]
        article['link'] = [i.text for i in self.soup.select('guid')]
        article['author'] = [i.find_next_sibling().text for i in self.soup.select('title')[1::]]
        article['summary'] = [i.text for i in self.soup.select('description')[1::]]
        article['pub_date'] = [datetime.datetime.strptime(i.text[:25], '%a, %d %b %Y %H:%M:%S') for i in self.soup.select('pubdate')]
        article['website'] = 'dev.to'

        return article
    
class Medium(Articles):
    def __init__(self, page):
        # super().__init__("Medium")
        self.soup = self.get_medium_soup(page)
        self.num_pages = 11
        self.article = self.get_df()
        # super().load_data(self.article, 'data_articles')

    def get_medium_soup(self, page):
        url = 'https://medium.com/_/graphql'

        headers = {
            'origin': 'https://medium.com',
            'referer': 'https://medium.com/',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36',
            'ot-tracer-spanid': '24330eb165f5d5c2',
            'ot-tracer-traceid': '607cd535383d11a6',
            'cookie': '_ga=GA1.2.1278478269.1631213857; __stripe_mid=db0731a9-e516-4003-a6ea-6659ac3415bc9aaa5a; nonce=hOcPGgoz; uid=846c46eb2386; sid=1:P4kJ8o6RXyMcpcvA/puobZ5mtCr8YT2UBcwSnplSZPOQva97REaYIbAdcE7flokl; lightstep_guid/medium-web=cf772ae8aea36f89; lightstep_session_id=e6fa630d0ba01a87; sz=1903; pr=1; tz=300; __cfruid=cb1a3237d7beb15d4bd4e19fff69fa2977a45875-1677681887; xsrf=715b337343e4; _gid=GA1.2.420747897.1677874872; _dd_s=rum=0&expire=1677878355669'
        }  

        with open('medium_query.txt') as t:
            query = t.read()

        variables = {"forceRank":False,
                    "paging":{
                        "limit":25,
                        "to": f"{page*25}"
                    }
                }

        r = requests.request("POST", url, headers=headers, json={'query': query, "variables": variables})

        soup = r.json()

        return soup

    def get_url(self):
        if self.page >= 1:
            url = f'https://dev.to/feed?page={self.page}'
        else:
            raise Exception('Page number must be above 0')
        
        return url
    
    def get_df(self):  
        articles = pd.json_normalize(self.soup['data']['webRecommendedFeed']['items'])

        article = pd.DataFrame()

        article['title'] = articles['post.title']
        article['link'] = articles['post.mediumUrl']
        article['summary'] = articles['post.extendedPreviewContent.subtitle']
        article['author'] = articles['post.creator.name']
        article['pub_date'] = [datetime.datetime.fromtimestamp(i/1000).date() for i in articles['post.firstPublishedAt']]
        article['website'] = 'Medium'

        return article

""" 
class DataScienceCentral(Articles):
    def __init__(self, page):
        # super().__init__("freeCodeCamp")
        self.page = page
        self.url = self.get_url()
        self.r = super().get_request(self.url, 'post')
        self.soup = BeautifulSoup(self.r.content.decode('utf-8'), 'html.parser')
        self.num_pages = self.get_pages()
        self.article = self.get_df()
        # super().load_data(self.article, 'data_articles')

    def get_pages(self):
        num_pages = 

        return num_pages

    def get_url(self):
        if self.page >= 1:
            url = f'https://www.datasciencecentral.com/wp-json/nv/v1/posts/page/{self.page}/en_US'
        else:
            raise Exception('Page number must be above 0')
        
        return url
    
    def get_df(self):
        article = pd.DataFrame()

        article['title'] = [i.text for i in self.soup.select("h2")]
        article['link'] = [i.select("a")[0]['href'][2:-4].replace('\\/', '/') for i in self.soup.select("h2")]
        article['author'] = [i.select('.meta-item')[0].text.strip() if len(i.select('.meta-item')) > 0 else "No Author" for i in self.soup.select('.post-card-meta')]
        article['pub_date'] = [datetime.datetime.strptime(i.select('time')[0]['datetime'][:24], '%a %b %d %Y %H:%M:%S') for i in self.soup.select('.post-card-meta')]
        article['website'] = 'freeCodeCamp'

        return article 
"""

# In[6]:

sf_test = StitchFix(1)
av_test = AnalyticsVidhya(1)
cp_test = CleverProgrammer(1)
fcc_test = freeCodeCamp(1)
rp_test = RealPython(1)
dt_test = devto(1)
# md_test = Medium(1)

