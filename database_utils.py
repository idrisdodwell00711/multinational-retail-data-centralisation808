#will use to connect with and upload data to the database.
from data_cleaning import DataCleaning
from data_extraction import DataExtractor
import pandas as pd
from sklearn.datasets import load_iris
from sqlalchemy import create_engine
from sqlalchemy import inspect
import yaml

class DatabaseConnector:
    def __init__(self, data):
        self.data = data
        
    def read_db_creds(file_name):
        with open(f'{file_name}', 'r') as file:
            prime_service = yaml.safe_load(file)
        return prime_service
    
    def init_db_engine(db_creds):
        
        host = db_creds['RDS_HOST']
        port= db_creds['RDS_PORT']
        database= db_creds['RDS_DATABASE']
        user= db_creds['RDS_USER']
        password= db_creds['RDS_PASSWORD']
        
        
        engine = create_engine(f"{'postgresql'}+{'psycopg2'}://{user}:{password}@{host}:{5432}/{database}")
        engine.execution_options(isolation_level='AUTOCOMMIT').connect()
        return engine
    
    def init_local_db_engine(db_creds):
        DATABASE_TYPE = db_creds['DATABASE_TYPE']
        DBAPI = db_creds['DBAPI']
        ENDPOINT = db_creds['ENDPOINT']
        USER = db_creds['USER']
        PASSWORD = db_creds['PASSWORD']
        PORT = db_creds['PORT']
        DATABASE = db_creds['DATABASE']
        
        engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{ENDPOINT}:{PORT}/{DATABASE}")
        engine.connect()
        return engine
        
    def list_db_tables( engine):
        inspector = inspect(engine)
        return inspector.get_table_names()
    
    def upload_to_db(engine, pd_df):
        #upload the df to db
        pd_df.to_sql('dim_card_details',engine, if_exists='replace', index = False)
        
#test = DatabaseConnector

# clean = DataCleaning
# df = DataExtractor
#test.download_from_db(test.read_db_creds('pgadmin_creds.yaml'), 'dim_card_details')
#test.upload_to_db(test.init_local_db_engine(test.read_db_creds('pgadmin_creds.yaml')),df.retrieve_pdf_data('https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf'))
#test.upload_to_db(test.init_local_db_engine(test.read_db_creds('pgadmin_creds.yaml')),clean.clean_card_data(df.retrieve_pdf_data('https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf')))
#test.upload_to_db(test.init_local_db_engine(test.read_db_creds('pgadmin_creds.yaml')),clean.clean_json(df.extract_from_json('https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json')))
#header = {'x-api-key':'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}
#test.upload_to_db(test.init_local_db_engine(test.read_db_creds('pgadmin_creds.yaml')), clean.clean_products_data(clean.convert_product_weights(df.extract_from_s3('s3://data-handling-public/products.csv'))))
#test.upload_to_db(test.init_local_db_engine(test.read_db_creds('pgadmin_creds.yaml')), clean.called_clean_store_data(df.retrieve_stores_data(df.list_number_of_stores('https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores', header), header)))

#test.upload_to_db(test.init_local_db_engine(test.read_db_creds('pgadmin_creds.yaml')), clean.clean_user_data(df.read_rds_table(test.list_db_tables(test.init_db_engine(test.read_db_creds('db_creds.yaml'))), test.init_db_engine(test.read_db_creds('db_creds.yaml')), 'users')))

#test.upload_to_db(test.init_db_engine(), clean.clean_user_data(df.read_rds_table(test.lis

#test.upload_to_db(test.init_local_db_engine(test.read_db_creds('pgadmin_creds.yaml')), clean.clean_user_data(df.read_rds_table(test.list_db_tables(test.init_local_db_engine(test.read_db_creds('pgadmin_creds.yaml'))), test.init_local_db_engine(test.read_db_creds('pgadmin_creds.yaml')), 'users')))

#test.upload_to_db(test.init_db_engine(), clean.clean_user_data(df.read_rds_table(test.list_db_tables(test.init_db_engine()), test.init_db_engine(), 'users')))
#test.upload_to_db(test.init_db_engine(), clean.clean_orders_data(df.read_rds_table(test.list_db_tables(test.init_db_engine()), test.init_db_engine(), 'orders')))  
#dim_card_details
#dim_store_details
        
        
# UPDATE dim_store_details
# 	SET weight_class =
# 		
# 		CASE WHEN weight < 2 then 'Light'
# 			 WHEN weight BETWEEN 2 AND 39 THEN 'Mid_Sized'
# 			 WHEN weight BETWEEN 40 AND 139 THEN 'Mid_Sized'
# 			 WHEN weight > 139 then 'Truck_Required'
# 			 END
# 	    

# UPDATE dim_products
# 	SET product_price = REPLACE(product_price, 'Â', '')
