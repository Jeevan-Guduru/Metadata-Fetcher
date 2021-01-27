#!/usr/bin/env python
# coding: utf-8

# In[2]:

import pandas as pd
from abc import ABC,abstractmethod
import os.path
import json
from configparser import ConfigParser
import psycopg2
import logging
import io
from sqlalchemy import create_engine
import sys
from sql_wrapper import db_connector


#log handler
logging.basicConfig(level=logging.INFO,format="%(asctime)s [%(levelname)s] %(message)s",handlers=[logging.FileHandler("debug.log"),logging.StreamHandler()])


#Factory class that lets choose the crawler type based on the file type given
class CrawlFactory:
    def get_crawler(self,file_path):
        filename, file_extension = os.path.splitext(file_path)
        file_extension=file_extension.lower()
        
        if file_extension=='.csv':
            return CSVcrawler()
        elif file_extension=='.json':
            return JSONcrawler()
        else:
             print("Parsing of this file type not supported!") #need to be changed try catch exception


                
#Abstract class whose method 'crawl' is implemented by file crawler classes below                
class Crawler(ABC):
   #constructor
    def __init__(self): pass
    @abstractmethod
    def crawl(self,file_path): pass



#CSV crawler that crawls data from the input csv file    
class CSVcrawler(Crawler):
    def __init__(self):
        logging.info("CSV file crawling is initialized")
    
    def crawl(self,file_path):
        df=pd.read_csv(file_path,low_memory=True)
        logging.info("CSV file crawling is completed")
        return df
    
    
    
#JSON crawler that crawls data from the input JSON file     
class JSONcrawler(Crawler):
    def __init__(self):
        logging.info("JSON file crawling is initialized")
    
    def crawl(self,file_path):
        with open(file_path) as project_file:    
            data = json.load(project_file)
        df = pd.json_normalize(data)
        ogging.info("JSON file crawling is completed")
        return df
    
    

#special class implemented to fetch metadata from the data-frame that is formed by crawlers above.
class MetadataFetcher():
    def __init__(self):
        logging.info("Metadata fetching is initialized")
    def fetch_metadata(self,df):
        df_metadata=pd.DataFrame(df.dtypes,columns=['Data_Type'])
        df_metadata.loc[df_metadata['Data_Type'] == 'int64', 'Data_Type'] = 'Integer'
        df_metadata.loc[df_metadata['Data_Type'] == 'float64', 'Data_Type'] = 'Float'
        df_metadata.loc[df_metadata['Data_Type'] == 'object', 'Data_Type'] = 'String'
        df_metadata['Null Count']=df.isnull().sum()
        df_metadata.loc[df_metadata['Null Count'] == len(df), 'Data_Type'] = 'NA(NULL)'
        df_metadata['Non-Null Count']=df.notnull().sum()
        df_metadata['Row-Count']=len(df)
        df_metadata.reset_index(inplace=True)
        df_metadata.rename(columns = {'index':'Columns'}, inplace = True)
        logging.info("Metadata fetching is Completed")
        return df_metadata

    
    
#Function that write fetched metadata to postgresql DB    
@db_connector
def write_df_sql(conn, arg1, arg2):
    logging.info("Writing metadata to DB...")
    table_name=arg2
    arg1.to_sql(table_name,conn, index = False, if_exists = 'replace', method="multi")
    logging.info("Metadata written to DB successfully!!")
    

    
#created a function -main which acts as start point for this application    
def main():
    #reading console arguments and validating them
    n = len(sys.argv)
    file_path=sys.argv[1]
    if n==2 and os.path.isfile(file_path):
        pass
    elif n!=2:
        logging.error("Invalid no of argumnets passed")
        sys.exit()
    else:
        logging.error("No such file or Directiry "+file_path)
        sys.exit()
    file_name=os.path.basename(file_path).replace('.','_')
    #factory object is created
    factory=CrawlFactory()
    #get_crawler method gives required crawler object(CSV/JSON) based on the file type
    crawler=factory.get_crawler(file_path)
    #crawl method of crawler object is called which returns a Data-Frame
    df=crawler.crawl(file_path)
    #Metadata fetcher object is created/instantiated
    metadataFetcher=MetadataFetcher()
    #passing df from above crawl method to fetch_metdata method of MetadataFetcher
    result=metadataFetcher.fetch_metadata(df)
    write_df_sql(result,file_name)
    

if __name__ == "__main__":
    main()