import argparse
import configparser
import sqlite3
from api2db_functions import *

# Global
PP_URL = 'https://projects.propublica.org/api-docs/congress-api/members/'
DB_NAME = 'congress.db'
CHAMBERS = ['senate', 'house']
API_DICT = {
    'id': 'TEXT',
    'first_name': 'TEXT',
    'middle_name': 'TEXT',
    'last_name': 'TEXT',
    'suffix': 'TEXT',
    'date_of_birth': 'DATE',
    'gender': 'TEXT',
    'party': 'TEXT',
    'state': 'TEXT',
}

# Get ProPublica API Key
# Register at https://www.propublica.org/datastore/api/propublica-congress-api
parser = argparse.ArgumentParser()
parser.add_argument('config_file', type=str)
parser.add_argument('--key', action='store_true')
args = parser.parse_args()
if args.key:
    api_key = args.config
else:
    config_path = args.config_file
    cf = configparser.ConfigParser()
    cf.read(config_path)
    api_key = cf.get('propublica', 'PROPUBLICA_API_KEY')

# Instantiate SQLite connection and database
conn = sqlite3.connect(DB_NAME)

# Set columns to extract from API
api_columns = [ col for col in API_DICT.keys() ]

# Set SQL columns for table
db_col_dict = API_DICT.copy()
db_col_dict['chamber'] = 'TEXT'
db_col_dict['congress'] = 'TEXT'

# Scrape range of congresses available for senate and house
senate_range, house_range = scrape_range(PP_URL)

# Create table
create_db(conn, db_col_dict)

# ETL
for chamber in CHAMBERS:
    if chamber == 'senate':
        working_range = senate_range
    else:
        working_range = house_range
    
    for n in working_range:
        mems = get_data(conn, n, api_key, api_columns, chamber)

        conn = sqlite3.connect(DB_NAME)
        cur = conn.cursor()
        cur.executemany('''INSERT INTO all_congress VALUES (?,?,?,?,?,?,?,?,?,?,?);''', mems)
        conn.commit()
        cur.close()