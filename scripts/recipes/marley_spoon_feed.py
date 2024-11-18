# %%

import re
from selenium import webdriver
from datetime import date, datetime
from google.cloud import bigquery

# %%

###################################################################################
# Selenium with javascript POST request
# Cannot use request due to GraphQL authentication.
# Process needs to run within website's environment
###################################################################################

class MarleySpoon:
    def __init__(self, start_week = date.today(), number_of_weeks = 1):

        self.CLIENT = bigquery.Client()
        self.DATASET = 'recipes'
        self.TABLE = 'marley_spoon'

        self.DRIVER = webdriver.Chrome()
        self.API_KEY = self.func_get_api_key()

        if isinstance(start_week, date):
            self.START_WEEK = datetime.strftime(start_week, '%Y-%m-%d')
        else:
            raise Exception("Please enter a date following the format of datetime.date(2024, 11, 9)")

        if isinstance(number_of_weeks, int):
            self.NUMBER_OF_WEEKS = number_of_weeks
        else:
            raise Exception("Value for number_of_weeks must be an integer.")
        
        self.PAYLOAD = {
            "operationName": "GetMenu_Web",
            "variables": {
                "imageSize": "MEDIUM",
                "startDate": self.START_WEEK,
                "numberOfWeeks": self.NUMBER_OF_WEEKS ## can change this to get mutiple weeks at once
            },
            "query": "query GetMenu_Web($numberOfWeeks: Int, $imageSize: ImageSizeEnum!, $startDate: Date) {\n  menu(numberOfWeeks: $numberOfWeeks, startDate: $startDate, supportedRecipeTypes: [STANDARD, PREMIUM, CORE_DOWN]) {\n    startOfWeek\n    recipes {\n      id\n      slug\n      title\n      subtitle\n      mealType\n      recipeType\n      category {\n        displayText\n        __typename\n      }\n      image(size: $imageSize) {\n        url\n        __typename\n      }\n      attributes {\n        key\n        __typename\n      }\n      extraFees {\n        label\n        quantity\n        totalAmount\n        __typename\n      }\n      recipeVariants {\n        id\n        slug\n        title\n        subtitle\n        mealType\n        recipeType\n        category {\n          displayText\n          __typename\n        }\n        image(size: $imageSize) {\n          url\n          __typename\n        }\n        attributes {\n          key\n          __typename\n        }\n        extraFees {\n          label\n          quantity\n          totalAmount\n          __typename\n        }\n        variantName\n        __typename\n      }\n      __typename\n    }\n    __typename\n  }\n}\n"
        }

        self.HEADERS = {
            "Authorization": f"Bearer {self.API_KEY}",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
            "Content-Type":"application/json",
            "Accept": "*/*",
            "Accept-Encoding": "gzip, deflate, br, zstd"
        }


    def func_get_entry_details(self, entry):
        return {
            'id': entry['id'],
            'title': entry['title'],
            'subtitle': entry['subtitle'],
            'link': 'https://marleyspoon.com/menu/' + entry['slug'],
            #'published_date': week,
            'meal_type': entry['mealType'],
            'image': entry['image']['url'],
            'attributes': [i['key'] for i in entry['attributes']]
        }


    def func_get_api_key(self):
        self.DRIVER.get('https://marleyspoon.com/menu')

        api_key = re.findall(r'api_token="([a-zA-Z0-9\.\-\_\&\%\$\#\@\!]*)";', self.DRIVER.page_source)[0]

        return api_key

    def func_get_data(self):
        feed = self.DRIVER.execute_script(f'''                    
            let globalData = fetch("https://api.marleyspoon.com/graphql", {{
                method: "POST",
                headers: {self.HEADERS},
                body: JSON.stringify({self.PAYLOAD}),
            }}).then((r) => {{return r.json();}})

            return globalData;
        ''')

        feed_entries = self.func_clean_feed(feed)

        ids_loaded = self.func_ids_in_database()

        filtered_entries = [entry for entry in feed_entries if entry['id'] not in ids_loaded]

        if len(filtered_entries) > 0:
            self.func_load_data(filtered_entries, 'append')

        return filtered_entries

    def func_clean_feed(self, feed):
        ### if working with multiple weeks at once
        if len(feed['data']['menu']) == 1:
            feed_entries = [self.func_get_entry_details(entry) for entry in feed['data']['menu'][0]['recipes']]
        else: ## flatten the list of lists
            import itertools
            feed_entries = list(itertools.chain(*[
                [self.func_get_entry_details(entry) for entry in feed['data']['menu'][week_number]['recipes']] 
                for week_number in range(0,len(feed['data']['menu']))
            ]))

        return feed_entries

    def func_close_driver(self):
        self.DRIVER.quit()

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

marley = MarleySpoon()

marley.loaded_feed = marley.func_get_data()