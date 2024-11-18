# %%

import feedparser
import re
from dateutil import parser
from tqdm import tqdm
from google.cloud import bigquery
from datetime import datetime
import json
import pandas as pd

# %%

###################################################################################
# RSS Feed
###################################################################################

class Bakeoff:
    def __init__(self):
        self.CLIENT = bigquery.Client()
        self.DATASET = 'recipes'
        self.TABLE = 'bakeoff'
        self.MAX_DATE = self.func_last_published_date()

    def func_get_entry_details(self, entry):
        return {
            'id': re.findall(r'&p=([0-9]{1,6})', entry.id)[0],
            'title': entry.title,
            'link': entry.link,
            'published_date': parser.parse(entry.published),
        }

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

    def func_convert_datetime(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        
    def func_last_published_date(self):
        query = f'''
        SELECT max(published_date) published_date
        FROM `impactful-post-292301.{self.DATASET}.{self.TABLE}`
        '''

        job = self.CLIENT.query(query)

        return [row.published_date for row in job.result()][0]

    def func_full_run(self):
        ###################################################################################
        # First Run

        # Because we need the number of pages to loop through
        # Start on page 2, get the number of pages, then loop through all pages.
        ###################################################################################
        url = "https://thegreatbritishbakeoff.co.uk/recipes/all/feed/?paged=2"
        feed = feedparser.parse(url)

        feed_entries = [self.func_get_entry_details(entry) for entry in feed.entries]

        num_pages = re.findall(r'Page [0-9]{1,3} of ([0-9]{1,3})', feed.feed.title)[0]

        for i in tqdm(range(1,int(num_pages)+1)):
            if i == 2:
                continue # already got those entries above

            url = f"https://thegreatbritishbakeoff.co.uk/recipes/all/feed/?paged={i}"
            feed = feedparser.parse(url)

            feed_entries += [self.func_get_entry_details(entry) for entry in feed.entries]

        json_data = json.dumps(feed_entries, default=self.func_convert_datetime)

        self.func_load_data(json.loads(json_data), 'truncate')

        return feed_entries

    def func_one_page_run(self):
        ###################################################################################
        # Ongoing Runs

        # No need to number of pages
        # Get the latest and add to table
        # In BQ, create table clustered by published_date
        ###################################################################################
        url = "https://thegreatbritishbakeoff.co.uk/recipes/all/feed/"
        feed = feedparser.parse(url)

        feed_entries = [self.func_get_entry_details(entry) for entry in feed.entries]

        filtered_entries = [ent for ent in feed_entries if ent['published_date'] > self.MAX_DATE]

        if len(filtered_entries) > 0:
            json_data = json.dumps(filtered_entries, default=self.func_convert_datetime)

            self.func_load_data(json.loads(json_data), 'append')

            self.MAX_DATE = self.func_last_published_date()

        return filtered_entries

# %%


bakeoff = Bakeoff()

bakeoff.added_feed_entries = bakeoff.func_one_page_run()