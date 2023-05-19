''' observations.py
Winter 2023
Web scraping tools for scraping avalanche observations from the Utah avalanche
center (UAC) website'''

#global packages
import pandas as pd
import os
import requests
import datetime
from bs4 import BeautifulSoup

AVY_URL = 'https://utahavalanchecenter.org'
OBS_EXT='/observations'

def get_obs(start_date, end_date):
    '''returns a dataframe of the observations on the UAC website. '''
    start_url = generate_url(start_date, end_date)
    page_num = 0
    observations = pd.DataFrame()
    while True:
        try:
            if page_num != 0:
                data_url = start_url + f"&page={page_num}"
            else:
                data_url = start_url
            page_obs_new = get_page_obs(data_url)
            observations = pd.concat([observations, page_obs_new])
            page_num += 1
        except:
            break
    observations = observations.reset_index(drop=True)
    #TODO: if no start date or end date give this season's observations
    return observations

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
    page_obs[['Observation Title', 'extension']]= pd.DataFrame(page_obs[old_columns[2]].tolist(), index=page_obs.index)
    page_obs[['Observer', 'd']]= pd.DataFrame(page_obs[old_columns[3]].tolist(), index=page_obs.index)
    
    #remove old columns & columns with none
    
    page_obs=page_obs.drop(old_columns, axis=1)
    page_obs=page_obs.drop(['a','b','d'], axis=1)
    return page_obs


def generate_url(start_date, end_date):
    '''generates the url for observations within a date range between start_date
    and end_date'''
    return f"https://utahavalanchecenter.org/observations?rid=All&term=All&fodv%5Bmin%5D%5Bdate%5D={start_date.month:02d}/{start_date.day:02d}/{start_date.year}&fodv%5Bmax%5D%5Bdate%5D={end_date.month:02d}/{end_date.day:02d}/{end_date.year}"

def str_to_datetime(str):
    '''converts a M/D/Y to datetime object'''
    date_list = date_str.split('/')
    return datetime.datetime(int(date_list[2]),int(date_list[0]),
            int(date_list[1])) 

def main():
	page_obs = get_page_obs(AVY_URL + OBS_EXT) 
	print(page_obs.head())


if __name__ == '__main__':
    main()
