#!/usr/bin/env python
# coding: utf-8

# In[ ]:
from configparser import ConfigParser
import psycopg2
import logging 
from sqlalchemy import create_engine


#log handler
logging.basicConfig(level=logging.INFO,format="%(asctime)s [%(levelname)s] %(message)s",handlers=[logging.FileHandler("debug.log"),logging.StreamHandler()])




#Postgresql Wrapper. Decorater to handle DB connection in functions that are wrapped by this.
def db_connector(func):
    def with_connection_(*args,**kwargs):
        params = config(r"C:\Users\JEEVAN\Desktop\Dump\archive\Database.ini")
        con_string='postgresql+psycopg2://'+params["user"]+':'+params["password"]+'@'+params["host"]+':'+params["port"]+'/'+params["database"]
        engine = create_engine(con_string)
        conn = engine.connect()
        logging.info('Connecting to the PostgreSQL database...')
        try:
            rv = func(conn, *args,**kwargs)
        except Exception:
            logging.error("Database connection error")
            raise
        else:
            logging.info("Done!")
        finally:
            conn.close()
            logging.info("DB connection closed.")
        return rv
    return with_connection_




#Function to read Database connection details from parameters file - Database.ini
def config(DBParamsPath='Database.ini', section='postgresql'):
    parser = ConfigParser()
    parser.read(DBParamsPath)
    db = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
    else:
        raise Exception('Section {0} not found in the {1} file'.format(section, filename))
    return db