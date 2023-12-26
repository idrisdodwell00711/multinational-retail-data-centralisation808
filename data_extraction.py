#The methods contained will be fit to
# extract data from a particular data source,
# these sources will include CSV files, an API and an S3 bucket.

import boto3
import numpy as np
import pandas as pd
import re
import requests
import tabula

class DataExtractor:
    def __init__(self, CSV, api, s3):
        self.CSV = CSV,
        self.api = api,
        self.s3 = s3
        
    def read_rds_table(db, engine, name_table):
        tables = db
        table_name = None
        for table in tables:
            if re.search(f'.*{name_table}*', table):
                table_name = table
                print(table_name, table)
        users = pd.read_sql_table(table_name,engine)
        return users
    
    def retrieve_pdf_data(link):
        pdf_path = link
        dfs = tabula.read_pdf(pdf_path, pages='all')
        
        #input all the disjoined pdf data into one reference dict
        df_pd_ref_dict = {'card_number':[], 'expiry_date':[], 'card_provider':[], 'date_payment_confirmed':[] }
        
        for page in dfs:
            for data in page:
                count = 0
                while count < len(dfs[0]):
                    df_pd_ref_dict[data].append(dfs[0].loc[count, data]) 
                    count = count +1   
        pd_df = pd.DataFrame(df_pd_ref_dict)

        return pd_df

    def list_number_of_stores(endpoint, header):
        response = requests.get(endpoint, headers = header)

            # Check if the request was successful (status code 200)
        if response.status_code == 200:
                # Access the response data as JSON
            data = response.json() 
            return data['number_stores']
        else:
            print(f"Request failed with status code: {response.status_code}")
            print(f"Response Text: {response.text}")
            
    def retrieve_stores_data(num_of_stores, header):
        #Obtains store num from function, makes an api req for each store, putting data in list
        if __name__ == 'data_extraction':
            store_list =[]
            store_num = 1
            while store_num <= (int(num_of_stores)-1):
                response = requests.get(f'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_num}', headers = header)
                store_list.append( response.json())
                store_num = store_num + 1
        
            pd_df = pd.DataFrame(store_list)
            
            return pd_df
    
    def extract_from_s3(s3_url):
        str_list = s3_url.split('/')
        s3 = boto3.resource('s3')
        s3.Bucket(str_list[2]).download_file(str_list[3],
        '/Users/Id/Documents/GitHub/multinational-retail-data-centralisation808/products.csv')
        f = open('/Users/Id/Documents/GitHub/multinational-retail-data-centralisation808/products.csv', 'r')
        pd_df = pd.read_csv(f)
        
        return pd_df
    
    def extract_from_json(url):
        #api req
        with requests.get(url, stream=True) as response:
            json_list = []
            if response.status_code == 200:
                # Access the response data as JSON
                data = response.json()
                response.close()
                key_list = data.keys()
                #normalise the json data and invert columns to rows for each column
                for column in key_list:
                    if column == 'timestamp':
                        pd_df = pd.json_normalize(data[column]).transpose()
                    else:
                        pd_df[column] = pd.json_normalize(data[column]).transpose()
             
                pd_df.columns = key_list
               
                return pd_df
            else:
                print('failed else state')
                print(f"Request failed with status code: {response.status_code}")
                print(f"Response Text: {response.text}")
        
       
#header = {'x-api-key':'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}       
        
#test = DataExtractor
#test.retrieve_stores_data(test.list_number_of_stores('https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores', header), header)
#test.extract_from_s3('s3://data-handling-public/products.csv')

#s3://data-handling-public/products.csv
#test.extract_from_json('https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json')
#test.retrieve_pdf_data('https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf')
# endpoint='https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'


