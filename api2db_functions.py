import requests
import re
import sqlite3
from bs4 import BeautifulSoup

# Function to create SQLite table
def create_db(conn, cols_dict):
    cols = [ f'{c} {d}' for c, d in cols_dict.items() ]
    cols = ', '.join(cols)
    c = conn.cursor()
    c.execute(
        f'''
        CREATE TABLE all_congress (
            {cols}
        )
        ;
        '''
    )
    c.close()

# Function to get the range of available congresses by chamber
def scrape_range(url):
    page = requests.get(url)
    soup = BeautifulSoup(page.content, 'html.parser')
    param_string = soup.find('td', string='congress').find_next_sibling().text

    senate_s = str(re.search(r'(?<=House, ).*(?= for Senate)', param_string)[0])
    house_s = str(re.search(r'.*(?= for H)', param_string)[0])
    senate_r = senate_s.split('-')
    house_r = house_s.split('-')
    senate_range = range(int(senate_r[0]), int(senate_r[1])+1)
    house_range = range(int(house_r[0]), int(house_r[1])+1)

    return senate_range, house_range

# Function to get single congress
def _get_congress(n, api_key, chamber):
    r = requests.get(
        f'https://api.propublica.org/congress/v1/{n}/{chamber}/members.json',
        headers={'X-API-Key': api_key}
    )
    results = r.json()['results'][0]['members']
    return results

# Function to trim data
def _trim_data(congress, api_columns):
    new_congress = []
    for mem in congress:
        new_mem = {}
        for col in api_columns:
            new_mem[col] = mem[col]
        new_congress.append(new_mem)
    return new_congress

# Function to get tuple of each member from a given congress
def _get_members(n, chamber, congress):
    members = [
        tuple(list(member.values()) + [f'{chamber}'] + [f'{n}']) for member in congress
    ]
    for member in members:
        assert len(member) == 11
    return members

# Function to insert data in SQL database
def get_data(conn, n, api_key, api_columns, chamber):
    congress = _get_congress(n, api_key, chamber)
    new_congress = _trim_data(congress, api_columns)
    members = _get_members(n, chamber, new_congress)
    
    return members