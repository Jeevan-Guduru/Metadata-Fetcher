# Metadata-Fetcher
This application is developed to gather metadata from given CSV and JSON files. 

## Modules:

crawl.py - Takes file path as input and parses the input file into pandas dataframe , fetches metadata out of it and stores the same into Postgresql DB.

describe.py - takes the file path as input, and reads the corresponding metadata from DB and prints on console.

sql_wrapper - this module has wrapper function for handling DB connections requested from above 2 modules.

Database.ini - this is a parameter file whcih has DB connection properties.

## Note: 
1.This application currently supports postgresql. Please change the connection properties accordingly in Database.ini, if you want to connect to your own postgresql.

2.Please make sure to keep this Database.ini file in the same directory where above modules are present.

3.JSON file type considered here is exactly like the one given in the assignment example.(list of objects). Other than list of objects this code do not have the capability to process.

## Executing:
Assuming python is installed, in command prompt, type as follow :

python path_to_code\crawl.py path_to_input_file\input_file.csv/json
python path_to_code\describe.py path_to_input_file\input_file.csv/json


