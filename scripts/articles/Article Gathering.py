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
import feedparser


# In[6]:


get_ipython().system('pip install feedparser --user')


# # Real Python

# In[ ]:


url = 'https://realpython.com/search/api/v2/?kind=article&order=newest'
r = requests.get(url, headers={"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.103 Safari/537.36"})
soup = BeautifulSoup(r.content.decode('utf-8'), 'html.parser')


# In[132]:


url = 'https://realpython.com/search/api/v2/?kind=article&order=newest'


# In[133]:


r = requests.request("GET", url)


# In[134]:


pages = math.ceil(r.json()['total']/20)
print(pages)


# In[193]:


real_python_articles = pd.DataFrame()


# In[197]:


for page in range(0,pages):
    print(f'Page being loaded {page+1}')
    url = f'https://realpython.com/search/api/v2/?kind=article&order=newest&continue_after={page*20}'
    
    r = requests.request("GET", url)
    
    page_results = pd.json_normalize(r.json()['results'])

    results = page_results[['title', 'url', 'description', 'pub_date']]
    results.columns = ['title', 'link', 'summary', 'pub_date']

    results['link'] = 'https://realpython.com' + results['link']

    results['pub_date'] = [datetime.datetime.strptime(i[:-6], "%Y-%m-%dT%H:%M:%S").date() if i != None else i for i in results['pub_date']]
    
    min_date = min(results[~pd.isna(results['pub_date'])]['pub_date'])
    
    if min_date <= datetime.datetime.strptime('2000-09-26', '%Y-%m-%d').date():
        break
    
    real_python_articles = pd.concat([real_python_articles, results])
    
    time.sleep(5)


# In[198]:


real_python_articles['website'] = 'Real Python'


# In[199]:


real_python_articles


# In[200]:


real_python_articles.to_csv('real_python.csv', index = False)


# 
# 
# # Medium

# In[526]:


url = 'https://medium.com/_/graphql'


# In[575]:


headers = {
    'origin': 'https://medium.com',
    'referer': 'https://medium.com/',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36',
    'ot-tracer-spanid': '24330eb165f5d5c2',
    'ot-tracer-traceid': '607cd535383d11a6',
    'cookie': '_ga=GA1.2.1278478269.1631213857; __stripe_mid=db0731a9-e516-4003-a6ea-6659ac3415bc9aaa5a; nonce=hOcPGgoz; uid=846c46eb2386; sid=1:P4kJ8o6RXyMcpcvA/puobZ5mtCr8YT2UBcwSnplSZPOQva97REaYIbAdcE7flokl; lightstep_guid/medium-web=cf772ae8aea36f89; lightstep_session_id=e6fa630d0ba01a87; sz=1903; pr=1; tz=300; __cfruid=cb1a3237d7beb15d4bd4e19fff69fa2977a45875-1677681887; xsrf=715b337343e4; _gid=GA1.2.420747897.1677874872; _dd_s=rum=0&expire=1677878355669'
    }

variables = {"forceRank":False,
             "paging":{
                 "limit":25,
                 "to": "50"
             }
         }


# In[576]:


with open('medium_query.txt') as t:
    query = t.read()


# In[587]:


medium_df_full = pd.DataFrame()


# In[588]:


for i in range(25,275,25):

    variables = {"forceRank":False,
                 "paging":{
                     "limit":25,
                     "to": f"{i}"
                 }
             }
    
    print(variables)
    
    r = requests.request("POST", url, headers=headers, json={'query': query, "variables": variables})
    
    df = pd.json_normalize(r.json()['data']['webRecommendedFeed']['items'])

    medium_df = pd.DataFrame()

    medium_df['title'] = df['post.title']
    medium_df['link'] = df['post.mediumUrl']
    medium_df['summary'] = df['post.extendedPreviewContent.subtitle']
    medium_df['author'] = df['post.creator.name']
    medium_df['pub_date'] = [datetime.datetime.fromtimestamp(i/1000).date() for i in pd.json_normalize(r.json()['data']['webRecommendedFeed']['items'])['post.firstPublishedAt']]
    
    medium_df_full = pd.concat([medium_df_full, medium_df])


# In[606]:


medium_df_full['website'] = 'Medium'


# In[607]:


medium_df_full.to_csv('medium.csv', index = False)


# 
# 
# # Stitch Fix

# In[28]:


url = 'https://multithreaded.stitchfix.com/blog/'
r = requests.get(url, headers={"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.103 Safari/537.36"})
soup = BeautifulSoup(r.text, 'html.parser')


# In[436]:


num_pages = re.search('Page: 1 of ([0-9]*)',soup.select(".page_number")[0].text).group(1)


# In[489]:


def get_stitch_articles(page):

    if page == 1:
        url = 'https://multithreaded.stitchfix.com/blog/'
    elif page > 1:
        url = f'https://multithreaded.stitchfix.com/blog/page/{page}/'
    else:
        raise Exception('Page number must be above 0')
    
    r = requests.get(url, headers={"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.103 Safari/537.36"})
    soup = BeautifulSoup(r.content.decode('utf-8'), 'html.parser')
    articles = soup.select(".post-listing")
    print(url)
    
    article = pd.DataFrame()

    article['title'] = [i.select(".h4")[0].text.replace('\n', '') for i in articles]
    article['link'] = ["https://multithreaded.stitchfix.com" + i.select(".h4 > a")[0]['href'] for i in articles]
    article['summary'] = [i.select("p")[0].text if len(i.select("p"))>0 else i.select(".post-byline")[0].next_sibling.strip() for i in articles]
    article['author'] = [i.select(".post-author")[0].text.strip() for i in articles]
    article['pub_date'] = [datetime.datetime.strptime(i.select("time")[0].text, '%B %d, %Y') for i in articles]
        
    max_date = max(article['pub_date'])
    min_date = min(article['pub_date'])
    
    return article, max_date, min_date
    
def stitch_df(articles_df, num_pages):
    for page in range(2,int(num_pages)+1):
        article, max_date, min_date = get_stitch_articles(page)

        articles_df = pd.concat([articles_df, article])
        
        min_date = min(article['pub_date'])

        if min_date <= datetime.datetime.strptime('2008-09-26', '%Y-%m-%d'):
            break

    return articles_df
    


# In[490]:


articles_df = pd.DataFrame()

article, max_date, min_date = get_stitch_articles(1)

articles_df = pd.concat([articles_df, article])

if min_date >= datetime.datetime.strptime('2008-09-26', '%Y-%m-%d'):
    articles_df = stitch_df(articles_df, num_pages)


# In[608]:


articles_df['website'] = 'Stitch Fix'


# In[609]:


articles_df.to_csv('stitch_fix.csv', index = False)


# 
# # Analytics Vidhya

# In[25]:


url = 'https://www.analyticsvidhya.com/blog-archive/'
r = requests.get(url, headers={"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.103 Safari/537.36"})
soup = BeautifulSoup(r.content.decode('utf-8'), 'html.parser')


# In[597]:


num_pages = soup.select(".page-numbers")[3].text


# In[598]:


def get_vidhya_articles(page):

    if page >= 1:
        url = f'https://www.analyticsvidhya.com/blog-archive/{page}/'
    else:
        raise Exception('Page number must be above 0')
    
    r = requests.get(url, headers={"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.103 Safari/537.36"})
    soup = BeautifulSoup(r.content.decode('utf-8'), 'html.parser')
    vid_article_list = soup.select(".list-card-content")
    print(url)
    
    vid_article = pd.DataFrame()

    vid_article['title'] = [i.select("h4")[0].text for i in vid_article_list]
    vid_article['link'] = [i.select("a")[0]['href'] for i in vid_article_list]
    vid_article['author'] = [i.select("h6 > a")[0].text for i in vid_article_list]
    vid_article['pub_date'] = [datetime.datetime.strptime(i.select("h6 > a")[0].next_sibling[2::], '%B %d, %Y') for i in vid_article_list]

    max_date = max(vid_article['pub_date'])
    min_date = min(vid_article['pub_date'])
    
    return vid_article, max_date, min_date
    
def vidhya_df(vid_articles, num_pages):
    for page in range(2,int(num_pages)+1):
        vid_article, max_date, min_date = get_vidhya_articles(page)

        vid_articles = pd.concat([vid_articles, vid_article])
        
        min_date = min(article['pub_date'])

        if min_date <= datetime.datetime.strptime('2008-09-26', '%Y-%m-%d'):
            break

    return vid_articles


# In[599]:


vid_articles = pd.DataFrame()

vid_article, max_date, min_date = get_vidhya_articles(1)

vid_articles = pd.concat([vid_articles, vid_article])

if min_date >= datetime.datetime.strptime('2008-09-26', '%Y-%m-%d'):
    vid_articles = vidhya_df(vid_articles, num_pages)


# In[610]:


vid_articles['website'] = 'Analytics Vidhya'


# In[611]:


vid_articles.to_csv('analytics_vidhya.csv', index = False)


# 
# # Clever Programmer

# In[704]:


url = 'https://thecleverprogrammer.com/machine-learning/page/1/'
r = requests.get(url, headers={"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.103 Safari/537.36"})
soup = BeautifulSoup(r.content.decode('utf-8'), 'html.parser')


# In[602]:


num_pages = soup.select(".page-numbers")[-2].text


# In[721]:


def get_clever_articles(page):

    if page >= 1:
        url = f'https://thecleverprogrammer.com/machine-learning/page/{page}/'
    else:
        raise Exception('Page number must be above 0')
    
    r = requests.get(url, headers={"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.103 Safari/537.36"})
    soup = BeautifulSoup(r.content.decode('utf-8'), 'html.parser')
    clev_article_list = soup.select(".entries")[0]
    print(url)
    
    clev_article = pd.DataFrame()

    clev_article['title'] = [i.select(".entry-title")[0].text.strip() for i in clev_article_list]
    clev_article['link'] = [i.select(".entry-title > a")[0]['href'] for i in clev_article_list]
    clev_article['author'] = [i.select(".meta-author")[0].text for i in clev_article_list]
    clev_article['pub_date'] = [datetime.datetime.strptime(i.select(".meta-date")[0].text, '%B %d, %Y') for i in clev_article_list]

    max_date = max(clev_article['pub_date'])
    min_date = min(clev_article['pub_date'])
    
    return clev_article, max_date, min_date
    
def clever_df(clev_articles, num_pages):
    for page in range(2,int(num_pages)+1):
        clev_article, max_date, min_date = get_clever_articles(page)

        clev_articles = pd.concat([clev_articles, clev_article])
        
        min_date = min(clev_article['pub_date'])

        if min_date <= datetime.datetime.strptime('2008-09-26', '%Y-%m-%d'):
            break

    return clev_articles


# In[722]:


clev_articles = pd.DataFrame()

clev_article, max_date, min_date = get_clever_articles(1)

clev_articles = pd.concat([clev_articles, clev_article])

if min_date >= datetime.datetime.strptime('2008-09-26', '%Y-%m-%d'):
    clev_articles = clever_df(clev_articles, num_pages)


# In[723]:


clev_articles['website'] = 'Clever Programmer'


# In[724]:


clev_articles.to_csv('clever_programmer.csv', index = False)


# 
# # freeCodeCamp

# In[89]:


url = 'https://www.freecodecamp.org/news/350'
r = requests.get(url, headers={"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.103 Safari/537.36"})
soup = BeautifulSoup(r.content.decode('utf-8'), 'html.parser')


# In[74]:


num_pages = 400


# In[124]:


def get_freeCodeCamp_articles(page):

    if page == 1:
        url = 'https://www.freecodecamp.org/news/'
    elif page >= 1:
        url = f'https://www.freecodecamp.org/news/{page-1}/'
    else:
        raise Exception('Page number must be above 0')
    
    r = requests.get(url, headers={"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.103 Safari/537.36"})
    soup = BeautifulSoup(r.content.decode('utf-8'), 'html.parser')
    
    print(url)
    
    freeCodeCamp_article = pd.DataFrame()

    freeCodeCamp_article['title'] = [i.text.strip() for i in soup.select(".post-card-title")]
    freeCodeCamp_article['link'] = ['https://www.freecodecamp.org' + i['href'] for i in soup.select(".post-card-title > a")]
    freeCodeCamp_article['author'] = [i.select('.meta-item')[0].text.strip() if len(i.select('.meta-item')) > 0 else "No Author" for i in soup.select('.post-card-meta')]
    freeCodeCamp_article['pub_date'] = [datetime.datetime.strptime(i.select('time')[0]['datetime'][:24], '%a %b %d %Y %H:%M:%S') for i in soup.select('.post-card-meta')]

    # max_date = max(freeCodeCamp_article['pub_date'])
    min_date = min(freeCodeCamp_article['pub_date'])
    
    if len(soup.select(".error-code")) == 0:
        max_date = max(freeCodeCamp_article['pub_date'])
    else:
        max_date = datetime.datetime.strptime('1900-01-01', '%Y-%m-%d')
    
    return freeCodeCamp_article, max_date, min_date
    
def freeCodeCamp_df(freeCodeCamp_articles, num_pages):
    for page in range(2,int(num_pages)+1):
        freeCodeCamp_article, max_date, min_date = get_freeCodeCamp_articles(page)
        
        if max_date <= datetime.datetime.strptime('2008-09-26', '%Y-%m-%d'):
            break

        freeCodeCamp_articles = pd.concat([freeCodeCamp_articles, freeCodeCamp_article])
        
        min_date = min(freeCodeCamp_article['pub_date'])

        if min_date <= datetime.datetime.strptime('2008-09-26', '%Y-%m-%d'):
            break

    return freeCodeCamp_articles


# In[125]:


freeCodeCamp_articles = pd.DataFrame()

freeCodeCamp_article, max_date, min_date = get_freeCodeCamp_articles(1)

freeCodeCamp_articles = pd.concat([freeCodeCamp_articles, freeCodeCamp_article])

if min_date >= datetime.datetime.strptime('2008-09-26', '%Y-%m-%d'):
    freeCodeCamp_articles = freeCodeCamp_df(freeCodeCamp_articles, num_pages)


# In[126]:


freeCodeCamp_articles['website'] = 'freeCodeCamp'


# In[127]:


freeCodeCamp_articles.to_csv('freeCodeCamp.csv', index = False)


# 
# # Dev.to

# In[3]:


url = 'https://dev.to/feed?page=1000'
dev_article_list = pd.json_normalize(feedparser.parse(url)['entries'])


# In[20]:


num_pages = 1000


# In[19]:


def get_dev_articles(page):

    if page >= 1:
        url = f'https://dev.to/feed?page={page}'
    else:
        raise Exception('Page number must be above 0')
    
    dev_article_list = pd.json_normalize(feedparser.parse(url)['entries'])
    print(url)
    
    dev_article = pd.DataFrame()

    dev_article['title'] = dev_article_list['title']
    dev_article['link'] = dev_article_list['link']
    dev_article['author'] = dev_article_list['author']
    dev_article['summary'] = dev_article_list['summary']
    dev_article['pub_date'] = [datetime.datetime.strptime(i[:25], '%a, %d %b %Y %H:%M:%S') for i in list(dev_article_list['published'])]

    max_date = max(dev_article['pub_date'])
    min_date = min(dev_article['pub_date'])
    
    return dev_article, max_date, min_date
    
def dev_df(dev_articles, num_pages):
    for page in range(2,int(num_pages)+1):
        dev_article, max_date, min_date = get_dev_articles(page)

        dev_articles = pd.concat([dev_articles, dev_article])
        
        min_date = min(dev_article['pub_date'])

        if min_date <= datetime.datetime.strptime('2008-09-26', '%Y-%m-%d'):
            break

    return dev_articles


# In[21]:


dev_articles = pd.DataFrame()

dev_article, max_date, min_date = get_dev_articles(1)

dev_articles = pd.concat([dev_articles, dev_article])

if min_date >= datetime.datetime.strptime('2008-09-26', '%Y-%m-%d'):
    dev_articles = dev_df(dev_articles, num_pages)


# In[22]:


dev_articles['website'] = 'dev.to'


# In[23]:


dev_articles.to_csv('devto.csv', index = False)


# 
# # Spotify
# Not working as expected

# In[62]:


url = 'https://engineering.atspotify.com/wp-admin/admin-ajax.php?max_page_number=40&action=get_blog_posts&category_id=all&paged=2&nonce=4c59b1724c'
r = requests.request("POST", url)
spotify_article_list = BeautifulSoup(r.json()['content'], 'html.parser')


# In[30]:


num_pages = r.json()['max_page_number']


# In[ ]:


def get_spotify_articles(page):

    if page >= 1:
        url = f'https://dev.to/feed?page={page}'
    else:
        raise Exception('Page number must be above 0')
        
    r = requests.request("POST", url)
    
    spotify_article_list = BeautifulSoup(r.json()['content'], 'html.parser')
    print(url)
    
    spotify_article = pd.DataFrame()

    spotify_article['title'] = [i.text for i in spotify_article_list.select('.info > h2 > a')]
    spotify_article['link'] = [i['href'] for i in spotify_article_list.select('.info > h2 > a')]
    spotify_article['summary'] = [i.text for i in spotify_article_list.select('.info > p')]
    spotify_article['pub_date'] = [datetime.datetime.strptime(i.text, '%B %d, %Y') for i in spotify_article_list.select('.date')]

    max_date = max(spotify_article['pub_date'])
    min_date = min(spotify_article['pub_date'])
    
    return spotify_article, max_date, min_date
    
def spotify_df(spotify_articles, num_pages):
    for page in range(2,int(num_pages)+1):
        spotify_article, max_date, min_date = get_spotify_articles(page)

        spotify_articles = pd.concat([spotify_articles, spotify_articles])
        
        min_date = min(spotify_article['pub_date'])

        if min_date <= datetime.datetime.strptime('2008-09-26', '%Y-%m-%d'):
            break

    return spotify_articles


# spotify_articles = pd.DataFrame()
# 
# spotify_article, max_date, min_date = get_spotify_articles(1)
# 
# spotify_articles = pd.concat([spotify_articles, spotify_article])
# 
# if min_date >= datetime.datetime.strptime('2008-09-26', '%Y-%m-%d'):
#     spotify_articles = spotify_df(spotify_articles, num_pages)

# 
# # Load to BQ

# In[65]:


real_python_articles = pd.read_csv('real_python.csv')
medium_df_full = pd.read_csv('medium.csv')
articles_df = pd.read_csv('stitch_fix.csv')
vid_articles = pd.read_csv('analytics_vidhya.csv')
clev_articles = pd.read_csv('clever_programmer.csv')
freeCodeCamp_articles = pd.read_csv('freeCodeCamp.csv')
dev_articles = pd.read_csv('devto.csv')

all_articles = pd.concat([real_python_articles, medium_df_full, articles_df, vid_articles, clev_articles, freeCodeCamp_articles, dev_articles])

all_articles['author'] = all_articles['author'].fillna('No Author')

print(all_articles)


# In[ ]:


all_articles.to_csv('all_articles.csv', index = False)


# In[ ]:




