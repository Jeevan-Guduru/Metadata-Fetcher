#!/usr/bin/env python
# coding: utf-8

# In[14]:

import pandas as pd
import os.path
import logging
import io
from sqlalchemy import create_engine
import sys
from sql_wrapper import db_connector



#log handler
logging.basicConfig(level=logging.INFO,format="%(asctime)s [%(levelname)s] %(message)s",handlers=[logging.FileHandler("debug.log"),logging.StreamHandler()])

# In[22]:


@db_connector
def read_table_sql(conn, arg1, arg2=None):
    table_name=str(arg1)
    #creating customised exception for reading metadata table
    try:
        result=pd.read_sql_query('select * from "'+ table_name+'"',con=conn)
    except Exception:
        logging.error("Metadata table for given file does not exist in DB.Please crawl the file and then describe!!")
        raise
    else:
        logging.info("Metadata table read successfully!!")
    finally:
        conn.close()
    return result


# In[60]:


def main():
    file_path=sys.argv[1]
    n = len(sys.argv)
    file_path=sys.argv[1]
    if n==2 and os.path.isfile(file_path):
        pass
    elif n!=2:
        logging.error("Invalid no of argumnets passed")
        sys.exit()
    else:
        logging.error("No such file or Directiry: "+file_path)
        sys.exit()
    file_name=os.path.basename(file_path).replace('.','_')
    result=read_table_sql(file_name)
    print("Total entries: "+ result.iloc[1,4].astype(str))
    print(result.iloc[:,:4])
    

# In[61]:

if __name__ == "__main__":
    main()