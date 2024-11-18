# %%

import requests
import json
import brotli
from bs4 import BeautifulSoup
from google.cloud import bigquery

# %%

##################################################
# Requests (GET) with brotli encoding
# Same as Hello Fresh
##################################################

class EveryPlate:
    def __init__(self):

        self.CLIENT = bigquery.Client()
        self.DATASET = 'recipes'
        self.TABLE = 'everyplate'

        self.ACCESS_TOKEN = self.func_get_access_token()

        self.HEADERS = {
            'Authorization': f'Bearer {self.ACCESS_TOKEN}', 
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36', 
            'Content-Type': 'application/json', 
            'Accept': '*/*', 
            'Accept-Encoding': 'br',
            'X-Requested-By': 'customer-clarity'
        }

        self.PARAMS = {
            "country": "ER",
            "locale": "en-US",
            "skip": 0,
            "sort": "-date",
            "take": 50
        }


    def func_get_entry_details(self, entry):
        return {
            'id': entry['id'],
            'title': entry['name'],
            'subtitle': entry['headline'],
            'link': entry['websiteUrl'],
            #'published_date': entry['updatedAt'],
            'category': entry['category']['name'] if entry['category'] is not None else '',
            'image': "https://img.everyplate.com/w_1200,q_auto,f_auto,c_fill,fl_lossy/hellofresh_s3" + entry['imagePath'],
            'cuisines': [i['name'] for i in entry['cuisines']]
        }

    def func_get_access_token(self):
        r = requests.get('https://www.everyplate.com/weekly-menu')

        soup = BeautifulSoup(r.content, 'html.parser')

        access_token = json.loads(soup.find("script", {"id": "__NEXT_DATA__"}).text)['props']['pageProps']['ssrPayload']['serverAuth']['access_token']

        return access_token

    def func_get_data(self):

        url = 'https://www.everyplate.com/gw/menus-service/menus'

        r = requests.get(url, headers=self.HEADERS, params=self.PARAMS)

        try:
            feed = r.json()['items']
        except:
            feed = json.loads(brotli.decompress(r.content))['items']

        feed_entries = self.func_clean_feed(feed)

        ids_loaded = self.func_ids_in_database()

        filtered_entries = [entry for entry in feed_entries if entry['id'] not in ids_loaded]

        if len(filtered_entries) > 0:
            self.func_load_data(filtered_entries, 'append')

        return filtered_entries

    def func_load_data(self, data, method):
        job_config = bigquery.LoadJobConfig()

        if method == 'append':
            job_config.write_disposition = bigquery.job.WriteDisposition.WRITE_APPEND
        elif method == 'truncate':
            job_config.write_disposition = bigquery.job.WriteDisposition.WRITE_TRUNCATE
        # The source format defaults to CSV, so the line below is optional.

        project = self.CLIENT.project
        dataset_id = bigquery.DatasetReference(project, self.DATASET)
        table_id = dataset_id.table(self.TABLE)

        job = self.CLIENT.load_table_from_json(data, table_id, job_config=job_config)  # Make an API request.
        job.result()  # Wait for the job to complete.

        print(f'Loaded table {self.TABLE}')

    def func_ids_in_database(self):
        query = f'''
            SELECT distinct id
            FROM `impactful-post-292301.{self.DATASET}.{self.TABLE}`
        '''

        job = self.CLIENT.query(query)

        if not job.errors:
            ids = [row.id for row in job.result()]
        else:
            if len([i for i in job.errors if i['reason'] == 'notFound']) > 0:
                ids = []
                print('Table does not exist')

        return ids
    
    def func_clean_feed(self, feed):
        ### if working with multiple weeks at once
        if len(feed) == 1:
            feed_entries = [self.func_get_entry_details(entry['recipe']) for entry in feed[0]['courses']]
        else: ## flatten the list of lists
            import itertools
            feed_entries = list(itertools.chain(*[
                [self.func_get_entry_details(entry['recipe']) for entry in feed[product]['courses']] 
                for product in range(0,len(feed))
            ]))

        return feed_entries


# %%

everyplate = EveryPlate()

# %%

everyplate.loaded_feed = everyplate.func_get_data()