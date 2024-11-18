# %%

import requests
from bs4 import BeautifulSoup
from datetime import date, datetime
from google.cloud import bigquery
import json

# %%

##################################################
# Requests (GET) with BeautifulSoup
##################################################

class HomeChef:
    def __init__(self, menu_week = None):

        self.CLIENT = bigquery.Client()
        self.DATASET = 'recipes'
        self.TABLE = 'homechef'

        if not menu_week:
            self.MENU_WEEK = ''        
        elif isinstance(menu_week, date):
            self.MENU_WEEK = datetime.strftime(menu_week, '%d-%b-%Y').lower()
        else:
            raise Exception("Please enter a date following the format of datetime.date(2024, 11, 9)")


    def get_entry_details(self, entry):
        return {
            'id': entry.find("a", {"aria-label": "meal details"})['href'].split('/')[-1],
            'title': entry.find("h1").text,
            'subtitle': entry.find("h1").find_next_sibling('p').text,
            'link': 'https://www.homechef.com/' + entry.find("a", {"aria-label": "meal details"})['href'],
            'menu_date': self.MENU_WEEK,
            'category': entry.find("img").find_next_sibling('p').text.strip() if entry.find("img").find_next_sibling('p') is not None else '',
            'image': entry.find("img")['src'],
            #'cuisines': [i['name'] for i in entry['cuisines']]
        }


    def func_get_data(self):
        r = requests.get(f'https://www.homechef.com/our-menus/{self.MENU_WEEK}')

        soup = BeautifulSoup(r.content, 'html.parser')

        self.MENU_WEEK = datetime.strptime(
            soup.find('option', {'selected': 'selected'})['value'].title() if soup.find('meta', {'property':'og:url'})['content'] == 'https://www.homechef.com/our-menus' else soup.find('meta', {'property':'og:url'})['content'].split("/")[-1], 
        '%d-%b-%Y')  

        cards = soup.findAll('li', {'id': 'meal'})

        feed_entries = [self.get_entry_details(entry) for entry in cards]

        ids_loaded = self.func_ids_in_database()

        filtered_entries = [entry for entry in feed_entries if entry['id'] not in ids_loaded]

        if len(filtered_entries) > 0:
            json_data = json.dumps(filtered_entries, default=self.func_convert_datetime)

            self.func_load_data(json.loads(json_data), 'append')

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
    
    def func_convert_datetime(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()

# %%

homechef = HomeChef()

# %%

homechef.loaded_feed = homechef.func_get_data()