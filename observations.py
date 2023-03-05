''' observations.py
Winter 2023
Web scraping tools for scraping avalanche observations from the Utah avalanche
center website'''

import pandas as pd
import os
import requests
from bs4 import BeautifulSoup

AVY_URL = 'https://utahavalanchecenter.org'
OBS_EXT='/observations'

def get_page_obs(url):
    '''returns a dataframe of avalanche observations from url. Data in df
    includes Date, Region, Avalanche/Observation, (url) extension, and
    observor'''
    page = requests.get(url)
    soup = BeautifulSoup(page.content, 'html.parser')
    tbl=soup.find("table")
    page_obs = pd.read_html(str(tbl), extract_links='all')[0]
    page_obs = clean_page_obs(page_obs)
    return page_obs

def clean_page_obs(page_obs):
    '''Cleans up the html parser's interpretation of the observation list'''
    old_columns = page_obs.columns
    #change names 
    page_obs[['Date', 'a']]= pd.DataFrame(page_obs[old_columns[0]].tolist(), index=page_obs.index)
    page_obs[['Region', 'b']]= pd.DataFrame(page_obs[old_columns[1]].tolist(), index=page_obs.index)
    page_obs[['Avalanche/Observation', 'extension']]= pd.DataFrame(page_obs[old_columns[2]].tolist(), index=page_obs.index)
    page_obs[['Observer', 'd']]= pd.DataFrame(page_obs[old_columns[3]].tolist(), index=page_obs.index)
    
    #remove old columns & columns with none
    
    page_obs=page_obs.drop(old_columns, axis=1)
    page_obs=page_obs.drop(['a','b','d'], axis=1)
    return page_obs

def main():
	page_obs = get_page_obs(AVY_URL + OBS_EXT) 
	print(page_obs.head())


if __name__ == '__main__':
    main()
