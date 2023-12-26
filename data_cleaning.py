import numpy as np
import pandas as pd
import re

class DataCleaning:
    def __init__(self, data):
        self.data = data
    def clean_user_data(df):
        if __name__ == 'data_cleaning':
            print('clean df main')
            #cleans number and dates
            pd_df = df
            regex_expression = '^(?:(?:\(?(?:0(?:0|11)\)?[\s-]?\(?|\+)44\)?[\s-]?(?:\(?0\)?[\s-]?)?)|(?:\(?0))(?:(?:\d{5}\)?[\s-]?\d{4,5})|(?:\d{4}\)?[\s-]?(?:\d{5}|\d{3}[\s-]?\d{3}))|(?:\d{3}\)?[\s-]?\d{3}[\s-]?\d{3,4})|(?:\d{2}\)?[\s-]?\d{4}[\s-]?\d{4}))(?:[\s-]?(?:x|ext\.?|\#)\d{3,4})?$'
            pd_df.loc[~pd_df['phone_number'].str.match(regex_expression), 'phone_number'] = np.nan

            date_regex_expression = "^\d{4}-\d{2}-\d{2}$"
            pd_df.loc[~pd_df['date_of_birth'].str.match(date_regex_expression), 'date_of_birth'] = np.nan
            pd_df.loc[~pd_df['join_date'].str.match(date_regex_expression), 'join_date'] = np.nan
            
            clean_pd_df = pd_df.dropna(axis = 0, how='any')
            
            return clean_pd_df
    
    def clean_orders_data(df):
        if __name__ == 'data_cleaning':
            pd_df = df
            clean_pd_df = pd_df.drop(['first_name', 'last_name', '1', 'level_0'], axis = 1)
            
            return clean_pd_df
        
    def clean_card_data(df):
        if __name__ == 'data_cleaning':
            #cleans dates
            pd_df = df
            
            date_regex_expression = "^\d{4}-\d{2}-\d{2}$"
            pd_df.loc[~pd_df['date_payment_confirmed'].str.match(date_regex_expression), 'date_payment_confirmed'] = np.nan
            
            # m_d_date_regex_expression = "^((0[1-9])|(1[0-2]))[\/\.\-]*((0[8-9])|(1[1-9]))$"
            # pd_df.loc[~pd_df['expiry_date'].str.match(m_d_date_regex_expression), 'expiry_date'] = np.nan
            
            return pd_df
        
    def called_clean_store_data(df):
        if __name__ == 'data_cleaning':
        
            pd_df = df
            pd_df = pd_df.drop('lat', axis=1)
        
            only_num_regex_expression = "(?<!\S)[^A-Za-z\s]+(?!\S)"
            date_regex_expression = "^\d{4}-\d{2}-\d{2}$"
            country_code_regex_expression = "^[^A-Za-z\r\n]*[A-Za-z]{2,4}$"
            
            pd_df.loc[~pd_df['staff_numbers'].str.match(only_num_regex_expression), 'staff_numbers'] = np.nan
            pd_df.loc[~pd_df['longitude'].str.match(only_num_regex_expression), 'longitude'] = np.nan
            pd_df.loc[~pd_df['latitude'].str.match(only_num_regex_expression), 'latitude'] = np.nan
            pd_df.loc[~pd_df['opening_date'].str.match(date_regex_expression), 'opening_date'] = np.nan
            pd_df.loc[~pd_df['country_code'].str.match(country_code_regex_expression), 'country_code'] = np.nan
            clean_pd_df = pd_df.dropna(axis = 0, how='any')
            
            type_convert_dict = {    
                                    'staff_numbers':int,
                                    'longitude':float,
                                    'latitude':float}
            
            type_correct_pd_df = clean_pd_df.astype(type_convert_dict)
            
            return type_correct_pd_df
        
    def convert_product_weights(df):
    
        if __name__ == 'data_cleaning':
            
                pd_df = df
                
                count = 0
                while count <=1852:
                        weight = pd_df.iloc[count, 3]
                        #removing misplaced or bad data in column
                        if type(weight) == type('0'):
                            if len(weight)>8 or re.search(" ", weight) or not re.search("[kg, g, ml]", weight):
                                pd_df.iloc[count, 3] = None
                                count = count+1
                                continue
                            #cleaning weight column
                            split_weight_list =  re.split(r'[g,m]',weight)
                            if not split_weight_list[0].endswith('k') | split_weight_list[0].endswith('K'):
                                pd_df.iloc[count, 3] = float(split_weight_list[0])/1000
                            else:
                                pd_df.iloc[count, 3] = float(split_weight_list[0][:-1])
                        count = count +1            
                return pd_df
                    
    def clean_products_data(df):
        if __name__ == 'data_cleaning':
            pd_df = df
            #pd_df['category']= pd_df['category'].replace('S1YB74MLMJ', 'C3NCA2CL35', 'WVPMHZP59U')
            #pd_df['removed']= pd_df['removed'].replace('T3QRRH7SRP ', 'BPSADIOQOK', 'H5N71TV8AY')
            
            count = 0
            while count <=1852:
                #converting bad date data from data added column into default 01/0001 value
                date = pd_df.iloc[count, 6]
                if not type(date) == type('str'):
                    pd_df.iloc[count, 6] = '01/0001'
                    count = count +1
                else:
                    count = count +1
                        
            date_regex_expression = "^\d{4}-\d{2}-\d{2}$"
            pd_df.loc[~pd_df['date_added'].str.match(date_regex_expression), 'date_added'] = np.nan
            clean_pd_df = pd_df.dropna(axis = 0, how='any')
            
            return clean_pd_df
    
    def clean_json(df):
        if __name__ == 'data_cleaning':
            pd_df = df
            regex_dict = {
                            'time_period':"['Evening, Midday, Morning, Late_Hours]{1,12}",
                            'month':"[0-9, 10-12]{1,2}",
                            'day':"[0-9, 10-19, 20-29, 30-31]{1,2}",
                            'year':"[1900-2100]"
                        }
                        
            for regex in regex_dict:
                
                pd_df.loc[~pd_df[regex].str.match(regex_dict[regex]), regex] = np.nan
                
            pd_df['time_period']= pd_df['time_period'].replace('EOHYT5T70F', None).replace('MZIS9E7IXD', None)
            
            #pd_df['date_uuid'] = pd_df['date_uuid'].replace('3A21WYQSY7', None)
            
            date_regex_expression = '^[0-9a-f]{8}-[0-9a-f]{4}-[0-5][0-9a-f]{3}-[089ab][0-9a-f]{3}-[0-9a-f]{12}$'
            pd_df.loc[~pd_df['date_uuid'].str.match(date_regex_expression), 'date_uuid'] = np.nan
            
            clean_pd_df = pd_df.dropna(axis = 0, how='any')
            
            return pd_df
    
    

# Step 4:
# Once complete insert the data into the sales_data database using your upload_to_db method storing it in a table named dim_products.