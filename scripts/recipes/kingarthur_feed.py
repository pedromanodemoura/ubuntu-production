# %%

import requests
from google.cloud import bigquery
import json
from tqdm import tqdm

# %%

##################################################
# Requests (GET) with BeautifulSoup
##################################################

class KingArthur:
    def __init__(self):

        self.CLIENT = bigquery.Client()
        self.DATASET = 'recipes'
        self.TABLE = 'king_arthur'

        self.HEADERS = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36',
            'Content-Type': 'application/x-www-form-urlencoded'
        }

        self.URL = 'https://hwpwewotf5-dsn.algolia.net/1/indexes/*/queries?x-algolia-agent=Algolia%20for%20JavaScript%20(4.22.1)%3B%20Browser%20(lite)%3B%20instantsearch.js%20(4.66.0)%3B%20JS%20Helper%20(3.16.3)&x-algolia-api-key=6860dedd7a22f80f3bcb0402d1c7215a&x-algolia-application-id=HWPWEWOTF5'


    def get_entry_details(self, entry):
        return {
            'id': entry['url'].split('/')[-1],
            'title': entry['title'],
            'link': 'https://www.kingarthurbaking.com' + entry['url'],
            'category': None if 'category_lvl0' not in entry.keys() else entry['category_lvl0'] if isinstance(entry['category_lvl0'], list) else [entry['category_lvl0']] ,
            'image': ['https://www.kingarthurbaking.com' + img for img in entry['thumbnail']],
        }

    def func_get_data_page(self, page):
        data = {
            "requests":[
                {
                    "indexName":"recipe_index",
                    "params":"facets=%5B%5D&query=*&tagFilters=&analytics=false",
                    "page": page
                }
            ]
        }

        r = requests.request("POST", url, json=data)

        data = r.json()['results'][0]

        feed_entries = [self.get_entry_details(entry) for entry in data['hits']]

        return feed_entries, data['nbPages']

    def func_get_data(self):
        feed_entries = []

        entries, pages = self.func_get_data_page(0)

        feed_entries.extend(entries)

        for page in tqdm(range(1,pages)):
            entries, pages = self.func_get_data_page(page)

            feed_entries.extend(entries)
            
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

# %%

king_arthur = KingArthur()

# %%

king_arthur.loaded_feed = king_arthur.func_get_data()