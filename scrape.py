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
'''functions that help other webscraping applications'''

def get_html_table(url):
    '''returns a dataframe of the url's html.table data'''
    page = requests.get(url)
    soup = BeautifulSoup(page.content, 'html.parser')
    tbl=soup.find("table")
    html_table= pd.read_html(str(tbl), extract_links='all')[0]
    return html_table

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


########### OBSERVATIONS ###############
'''functions that grabs a list of observations (avlanche and general) with urls,
region, and date'''

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
''' scrapes avalanche observation webpage for data and stores in dataframe'''

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

#TODO Store data in temporary csv for repeated use


############## General Observation Parsers ########################

FIELDS_DICT= {} #group: [list of fields]
FIELDS_DICT['General'] = ['Observer Name', 'Observation Date', 'Region', 'Location Name or Route','Comments']
FIELDS_DICT['Weather'] = ['Sky', 'Wind Direction', 'Wind Speed', 'Weather Comments']
# "Snow surface conditions" may have multiple entries
FIELDS_DICT['Snow Characteristics'] = ['New Snow Depth', 'New Snow Density', 'Snow Surface Conditions', 'Snow Characteristics Comments'] 
# "Red Flags" may have multiple entries
FIELDS_DICT['Red Flags']=['Red Flags', 'Red Flags Comments']
FIELDS_DICT['Avalanche Problem #1'] = ['Problem', 'Trend', 'Problem #1 Comments']
FIELDS_DICT['Avalanche Problem #2'] = ['Problem', 'Trend', 'Problem #2 Comments']
#containts image
FIELDS_DICT['Snow Profile'] = ['Aspect', 'Elevation', 'Slope Angle']

multi_entry_fields = ['Red Flags', 'Snow Surface Conditions']

def get_observation_data(observation_table, verbose = False):
    '''Takes a dataframe containing a list of links to general observations and returns
    the data from each of those observations in a dataframe'''
    general_observation_table = filter_general_observations(observation_table).reset_index(drop=True)
    extensions = list(general_observation_table.extension)
    print('Gathering Data from', len(extensions), 'general observations.')
    
    data = []
    err = [] #collect list of extensions that did not work
    for extension in extensions:
        try:
            new_data = read_general_observation(AVY_URL + extension, verbose)
            data.append(new_data)
        except:
            err.append(extension)
    gen_obs_data = pd.DataFrame(data)
    return gen_obs_data, err

def read_multiple_entries(field, soup):
    '''returns a list of field of entries for a multi-entry field such as Red Flags and Snow Surface Conditions'''
    if field == 'Red Flags':
        parent_soup = soup.find_all(string=field)[1].parent
    else:
        parent_soup = soup.find(string=field).parent
    current_entry = parent_soup.next_sibling.next_sibling
    
    field_entries = []
    read_entries = True
    while read_entries:
        #print(current_entry)
        field_entries.append(current_entry.string)
        current_entry = current_entry.next_sibling.next_sibling
        #check to see if we ran into comments commonly found below a field
        if len(current_entry.string) > 20:
            read_entries = False
    return field_entries

def read_field_entry(field, soup, verbose=False):
    '''read the field entry for the given observation soup'''
    try:
        field_entry = soup.find(string=field).parent.next_sibling.next_sibling.string
        #print('Parent Field tag', soup.find(string=field).parent)
        #print('Field Entry tag', soup.find(string=field).parent.next_sibling.next_sibling)
    except:
        field_entry = None
        #print("Error at field:", field)
    #TODO:fix slope angle and numberic fields
    #if field in numeric_fields:
        #field_entry = convert_to_numeric(field_entry, field)
    return field_entry

def filter_general_observations(observations):
    '''filters pandas dataframe for just general observation entries'''
    return observations.loc[observations['Observation Title'].str.split(':').str[0] == 'Observation']

def read_general_observation(url, verbose = False):
    '''Extracts avalanche data from avalanche reports'''
    if verbose:
        print(url)
    page = requests.get(url)
    soup = BeautifulSoup(page.content, 'html.parser')
    
    numeric_fields = ['Elevation', 'Slope Angle']
    multi_entry_fields = ['Red Flags', 'Snow Surface Conditions']
    
    observation_data = {}
    #get observation title
    title_class='page-title'
    observation_title = soup.find(class_='page-title').string
    

    field_groups = list(FIELDS_DICT.keys())
    for group in field_groups:
        if verbose:
            print('\n',group,'Group:')
        for field in FIELDS_DICT[group]:
            if field in multi_entry_fields:
                entry = read_multiple_entries(field, soup)
            #TODO: extract image url from snow profile
            else:
                entry = read_field_entry(field, soup)
            if verbose:
                    print(field, ':',entry)
            observation_data[field] = entry
    return observation_data

############## FORECASTS ##################
''' gathers previous forecast data '''

def get_page_forecasts(url):
    '''Returns a clean dataframe of the url page's forecast table'''
    #get raw table
    page_casts = get_html_table(url)
    ##Start cleaning
    old_columns = page_casts.columns
    #change names 
    page_casts[['Date', 'a']]= pd.DataFrame(page_casts[old_columns[0]].tolist(), index=page_obs.index)
    page_casts[['Forecast Title', 'extension']]= pd.DataFrame(page_casts[old_columns[1]].tolist(), index=page_obs.index)
    page_casts[['Forecaster', 'd']]= pd.DataFrame(page_casts[old_columns[2]].tolist(), index=page_obs.index)
    #remove old columns & columns with none
    page_casts=page_casts.drop(old_columns, axis=1)
    page_casts=page_casts.drop(['a','b','d'], axis=1)


############################################

def main():
    print('~~ Welcome to the Utah Avalanche Data Scrape ~~') 
    start_date = TODAY - datetime.timedelta(days = 14)
    print('Looking for observations between', TODAY, 'and', start_date)
    obs_table =  get_observation_table(start_date=start_date)
    print('Found', len(obs_table), 'observations.')
    if len(obs_table) > 500:
        print('Too many observations to scan right now...closing application.')
        return
    print(obs_table.head())
    
    print('Scraping avalanche data...\n')
    avalanches, err = get_avalanche_data(obs_table)
    print(avalanches.head())

    print('Scraping general observation data...\n')
    observations, err = get_observation_data(obs_table, verbose=False)
    print(observations.head())
    return

main()
