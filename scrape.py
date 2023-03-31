'''
scrape.py

by Ethan Turner

Utils for scraping data from the Utah Avalanche Center.

'''
import requests
from bs4 import BeautifulSoup
import pandas as pd
import datetime
import re

AVY_URL = 'https://utahavalanchecenter.org'
OBS_EXT='/observations'
FOR_EXT='/archives/forecasts'

TODAY = datetime.date.today()

##### General ######

def get_html_table(url):
    '''returns a dataframe of the url's html.table data'''
    page = requests.get(url)
    soup = BeautifulSoup(page.content, 'html.parser')
    tbl=soup.find("table")
    html_table= pd.read_html(str(tbl), extract_links='all')[0]
    return html_table

########### OBSERVATIONS ###############

def get_page_obs(url):
    '''Returns a clean dataframe of the url page's observation table'''
    #get raw table
    page_obs = get_html_table(url)
    ##Start cleaning
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

def get_season_start(date):
    '''returns the beginning of the season as defined between the last August and the end of July'''
    start_day = 1
    start_month = 8
    end_month = 7
    if date.month < 7:
        start_year = date.year-1
    else:
        start_year = date.year
    return datetime.datetime(start_year, start_month, start_day)

def get_observation_table(start_date= get_season_start(TODAY), end_date=TODAY):
    '''returns a dataframe of the observations on the UAC website. '''
    start_url = generate_observation_url(start_date, end_date)
    page_num = 0
    observations_table = pd.DataFrame()
    while True:
        try:
            if page_num != 0:
                data_url = start_url + f"&page={page_num}"
            else:
                data_url = start_url
            page_obs_new = get_page_obs(data_url)
            observations_table = pd.concat([observations_table, page_obs_new])
            page_num += 1
        except:
            #if the page can't be loaded then we've reached the end of available obs
            break
    observations_table = observations_table.reset_index(drop=True)
    return observations_table

def generate_observation_url(start_date, end_date):
    return f"https://utahavalanchecenter.org/observations?rid=All&term=All&fodv%5Bmin%5D%5Bdate%5D={start_date.month:02d}/{start_date.day:02d}/{start_date.year}&fodv%5Bmax%5D%5Bdate%5D={end_date.month:02d}/{end_date.day:02d}/{end_date.year}"


#TODO filter by region


#### Avalache Observation Parsers #####

def get_avalanche_data(observation_table):
    '''Returns dataframe of avalanche data from list of observations'''
    avalanche_observation_table = filter_avalanche(observation_table).reset_index(drop=True)
    extensions = list(avalanche_observation_table.extension)
    data = []
    err = [] #collect list of extensions that did not work
    for extension in extensions:
        try:
            new_data = read_avalanche_observation(AVY_URL + extension)
            data.append(new_data)
        except:
            err.append(extension)
    avy_data = pd.DataFrame(data) 
    return avy_data, err

def read_avalanche_observation(url, verbose = False):
    '''Extracts avalanche data from avalanche reports'''
    page = requests.get(url)
    soup = BeautifulSoup(page.content, 'html.parser')
    title_class='page-title'
    fields = ['Observer Name', 'Observation Date', 'Region', 'Location Name or Route', 'Snow Profile','Comments']
    avy_fields = ['Elevation', 'Aspect', 'Trigger', 'Depth', 'Width', 'Carried']
    numeric_fields = ['Elevation', 'Depth', 'Width']
    avalanche_data = {}
    observation_title = soup.find(class_='page-title').string
    if observation_title.split(':')[0] == 'Avalanche':
        fields = fields + avy_fields
        for field in fields:
            try:
                datum = soup.find(string=field).parent.next_sibling.next_sibling.string
                if field in numeric_fields:
                    datum = convert_to_numeric(datum, field)
            except:
                datum = None
            if verbose:
                print(field, 'is:',datum)
            avalanche_data[field] = datum
    return avalanche_data

def filter_avalanche(observations):
    '''filters pandas dataframe for just avalanche entries'''
    return observations.loc[observations['Observation Title'].str.split(':').str[0] == 'Avalanche']

def convert_to_numeric(raw_string, field):
    r = re_fields[field]
    m = r.match(raw_string)
    if field == 'Elevation':
        return float(m.group(1))*1000 + float(m.group(2))
    else:
        return float(m.group(1))
############## FORECASTS ##################

def get_forecast_table():
    return get_html_table(AVY_URL + FOR_EXT)


def clean_forcast_table(forcast_table):
    '''Cleans up the html parser's interpretations of the forecast list'''
    

############################################

def main():
    print('Scraping avalanche data from Utah Avalanche Center for this Season...\n')
    season_observations =  get_observation_table() #defaults to this season
    print(season_observations.head())
    season_avalanches, err = get_avalanche_data(season_observations)
    print(season_avalanches.head())
    return

main()
