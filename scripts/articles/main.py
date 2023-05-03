#!/usr/bin/env python
# coding: utf-8

# In[7]:


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
    def __init__(self, website):
        # self.website = website
        pass

    # @abstractmethod
    def get_soup(self, url):
        r = requests.get(url, headers={"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.103 Safari/537.36"})

        try:
            soup = r.json()
        except:
            soup = BeautifulSoup(r.content.decode('utf-8'), 'html.parser')

        return soup

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
        self.soup = super().get_soup(self.url)
        self.article = self.get_df()

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
        self.soup = super().get_soup(self.url)
        self.article = self.get_df()

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
        self.soup = super().get_soup(self.url)
        self.article = self.get_df()

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
        self.soup = super().get_soup(self.url)
        self.article = self.get_df()

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

# class devto(Articles):
# class RealPython(Articles):
# class Medium(Articles):
# class DataScienceCentral(Articles):

# In[6]:

sf_test = StitchFix(1)
av_test = AnalyticsVidhya(1)
fcc_test = freeCodeCamp(1)
# %%

sf_test.article.head()

# %%
av_test.article.head()

# %%
fcc_test.article.head()


# %%
