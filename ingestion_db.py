import pandas as pd
import os
import time
from sqlalchemy import create_engine
import matplotlib.pyplot as plt

import logging

logging.basicConfig(
    filename = "logs/ingestion_db.log",
    level = logging.DEBUG,
    format = "%(asctime)s - %(levelname)s - %(message)s",
    filemode = "a"
    
)


engine = create_engine('sqlite:///inventory.db')

def ingest_db(df, table_name, engine):
    ''' This function will ingest the dataframe into the database table'''
    
    df.to_sql(table_name, con = engine , if_exists = 'replace',index = False )  #if_exists = append if you want to append the data
    

def load_raw_data():

    '''  this function will load csv's as dataframe and ingest them into the database'''
    
    start = time.time()
    for file in os.listdir('data'):
        if '.csv' in file:
            df = pd.read_csv('data/'+file)
            logging.info(f'Ingesting {file} in database')
            ingest_db(df, file[:-4], engine)  # file[:-4] - it removes .csv from the file name and save it as the table name
    end = time.time()
    total_time = (end - start) / 60
    
    logging.info('---------Ingestion Complete------------')     
    logging.info(f'Total time taken : {total_time} mins')


if __name__ == '__main__':
    load_raw_data()