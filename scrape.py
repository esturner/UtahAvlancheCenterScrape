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
from time import time

#image analysis
from PIL import Image

##### Constants ########

AVY_URL = 'https://utahavalanchecenter.org'
OBS_EXT='/observations'
FOR_EXT='/archives/forecasts'

TODAY = datetime.date.today()

#Fields of data that can be extracted from observations
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
FIELDS_DICT['Avalanche'] = ['Elevation', 'Aspect', 'Trigger', 'Depth', 'Width', 'Carried']

MULTI_ENTRY_FIELDS = ['Red Flags', 'Snow Surface Conditions']

NUMERIC_FIELDS = ['Elevation', 'Depth', 'Width', 'New Snow Depth', 'Slope Angle']

FIELD_UNITS= {}
FIELD_UNITS['Elevation'] = "\'"
FIELD_UNITS['Depth'] = "\""
FIELD_UNITS['Width'] = "\'"
FIELD_UNITS['Slope Angle'] = "°"
FIELD_UNITS['New Snow Depth'] = "\""

RE_UNITS= {}
RE_UNITS['Elevation']=re.compile(r"([0-9]*),([0-9]*)'")
RE_UNITS['\"'] = re.compile(r"([0-9]*|[0-9]*.[0-9]*)\"")
RE_UNITS['\''] = re.compile(r"([0-9]*|[0-9]*.[0-9]*)\'")
RE_UNITS['°'] = re.compile(r"([0-9]*)°")




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
    return datetime.date(start_year, start_month, start_day)

def split_subregions(region_str):
    '''splits the region format "Salt Lake » Big Cottonwood » Wolverine Peak" into ['Salt Lake', 'Big Cottonwoord', 'Wolverine Peak']'''
    region_list = [x.strip() for x in region_str.split('»')]
    return region_list

def get_forecast_region(region_str):
    subregions = split_subregions(region_str)
    forecast_region = subregions[0]
    return forecast_region


########### OBSERVATION TABLE ###############
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

def get_avalanche_data(start_date = get_season_start(TODAY), end_date = TODAY, verbose = False, timed=False):
    '''Returns dataframe of avalanche data from list of observations'''
    time_start = time()
    observation_table = get_observation_table(start_date=start_date, end_date=end_date) 
    avalanche_observation_table = filter_avalanche(observation_table).reset_index(drop=True)
    extensions = list(avalanche_observation_table.extension)
    data = []
    err = [] #collect list of extensions that did not work
    
    if verbose:
        print('Scraping', len(extensions), 'avalanche observations...')
    for extension in extensions:
        try:
            new_data = read_avalanche_observation(AVY_URL + extension, verbose)
            data.append(new_data)
        except:
            err.append(extension)
    avy_data = pd.DataFrame(data)
    
    #reformat data
    avy_data['Observation Date'] = avy_data['Observation Date'].apply(lambda x: datetime.datetime.strptime(x, '%A, %B %d, %Y').date())
    avy_data['Forecast Region'] = avy_data.Region.apply(get_forecast_region)
    time_stop = time()
    if timed:
        print("Time elapsed scraping avalanche data:", (time_stop-time_start)/60, 'min.')
    return avy_data, err

def read_avalanche_observation(url, verbose = False):
    '''Extracts avalanche data from avalanche reports'''
    page = requests.get(url)
    soup = BeautifulSoup(page.content, 'html.parser')
    fields = ['Observer Name', 'Observation Date', 'Region', 'Location Name or Route', 'Snow Profile','Comments']
    avy_fields = ['Elevation', 'Aspect', 'Trigger', 'Depth', 'Width', 'Carried']
    
    avalanche_data = {}
    observation_title = soup.find(class_='page-title').string
    if observation_title.split(':')[0] == 'Avalanche':
        fields = fields + avy_fields
        for field in fields:
            try:
                datum = soup.find(string=field).parent.next_sibling.next_sibling.string
                if field in NUMERIC_FIELDS:
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

######### Numeric Data ############

def contains_digit(raw_string):
    '''returns True if a digit is in string'''
    return bool(re.search(r'\d', raw_string))

def convert_to_numeric(raw_string, field):
    '''converts a numeric field entry into a float with the correct magnetude for the field unit (see constants)'''
    #check if there are any numerics
    if not contains_digit(raw_string):
        return None
    unit = raw_string[-1]
    field_unit = FIELD_UNITS[field]
    #print("The scraped unit and field unit is", unit, 'and', field_unit)
    #unit is given in ' and should be "
    if unit != field_unit:
        raw_string = re.sub(unit, field_unit, raw_string)
        #print(raw_string)
    
    if field == 'Elevation':
        r = RE_UNITS['Elevation']
        m = r.match(raw_string)
        num = float(m.group(1))*1000 + float(m.group(2))
    else:
        r = RE_UNITS[field_unit]
        m = r.match(raw_string)
        num = float(m.group(1))
    
    #now convert the wrong unit into inches
    if unit != field_unit:
        num = convert_to_inches(num)
    #print('The raw string and converted number are:',raw_string, num)
    
    return num

def convert_to_inches(l_feet):
    '''converts the length in feet to inches'''
    return l_feet*12

#TODO Store data in temporary csv for repeated use


############## General Observation Parsers ########################

def get_observation_data(start_date = get_season_start(TODAY), end_date=TODAY, verbose = False, timed = False):
    '''Takes a dataframe containing a list of links to general observations and returns
    the data from each of those observations in a dataframe'''
    observation_table = get_observation_table(start_date=start_date, end_date=end_date) 
    general_observation_table = filter_general_observations(observation_table).reset_index(drop=True)
    extensions = list(general_observation_table.extension)
    #print('Gathering Data from', len(extensions), 'general observations.')
    time_start = time()
    data = []
    err = [] #collect list of extensions that did not work
    for extension in extensions:
        try:
            new_data = read_general_observation(AVY_URL + extension, verbose)
            data.append(new_data)
        except:
            err.append(extension)
    gen_obs_data = pd.DataFrame(data)
    
    #reformat data
    gen_obs_data['Observation Date'] = gen_obs_data['Observation Date'].apply(lambda x: datetime.datetime.strptime(x, '%m/%d/%Y').date())
    gen_obs_data['Forecast Region'] = gen_obs_data.Region.apply(get_forecast_region)
    time_stop = time()
    if timed:
        print("Time elapsed scraping general observations:", (time_stop-time_start)/60, 'min.')
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
    
    if field in NUMERIC_FIELDS:
        field_entry = convert_to_numeric(field_entry, field)
    return field_entry

def filter_general_observations(observations):
    '''filters pandas dataframe for just general observation entries'''
    #print(observations.head())
    #print(observations['Observation Title'].str.split(':').str[0] == 'Observation')
    return observations.loc[observations['Observation Title'].str.split(':').str[0] == 'Observation']

def read_general_observation(url, verbose = False):
    '''Extracts avalanche data from avalanche reports'''
    if verbose:
        print(url)
    page = requests.get(url)
    soup = BeautifulSoup(page.content, 'html.parser')
    
    numeric_fields = ['Elevation', 'Slope Angle']
    MULTI_ENTRY_FIELDS = ['Red Flags', 'Snow Surface Conditions']
    
    observation_data = {}
    #get observation title
    title_class='page-title'
    observation_title = soup.find(class_='page-title').string
    

    field_groups = list(FIELDS_DICT.keys())
    for group in field_groups:
        if verbose:
            print('\n',group,'Group:')
        for field in FIELDS_DICT[group]:
            try:
                if field in MULTI_ENTRY_FIELDS:
                    entry = read_multiple_entries(field, soup)
                #TODO: extract image url from snow profile
                else:
                    entry = read_field_entry(field, soup)
            except:
                entry = None
            if verbose:
                    print(field, ':',entry)
            observation_data[field] = entry
    return observation_data

############## FORECASTS ##################
''' gathers forecast data from Utah Avalanche Centery Forecast Archive'''

def get_forecasts(start_date= get_season_start(TODAY), end_date=TODAY, region= 'Salt Lake', timed=False):
    '''returns dataframe of forecast fieds from the start_date to end date. Defaults to season's forecasts for
    salt lake'''
    time_start = time()
    date = start_date
    forecasts = pd.DataFrame()
    while date <= TODAY:
        #print(date)
        date_forecast = get_forecast(date, region)
        #print(date_forecast)
        forecasts = pd.concat([forecasts,date_forecast], axis=1)
        date += datetime.timedelta(days=1)
    forecasts=forecasts.T.reset_index(drop=True)
    time_stop = time()
    if timed:
        print("Time elapsed scraping forecasts:", (time_stop-time_start)/60, 'min.')
    return forecasts

def get_forecast(date, region):
    '''Returns a dataframe entry holding: Date, Region, Danger Rose Dictionary, Danger Rose image url,
    and numerated avalanche problems (whose data is stored in dictionaries).'''
    forecast_url = generate_forecast_url(date, region)
    forecast = pd.Series(dtype='object')
    forecast['Date'] = date
    forecast['Region'] = region
    danger_rose, rose_url = get_danger_rose(forecast_url)
    forecast['Danger Rose'] = danger_rose
    forecast['Danger Rose URL'] = rose_url
    avalanche_problems = avalanche_problems_to_series(read_avalanche_problems(forecast_url))
    forecast = pd.concat([forecast, avalanche_problems])
    return forecast

def generate_forecast_url(date, region):
    return f"https://utahavalanchecenter.org/forecast/{region.replace(' ', '-').lower()}/{date.month}/{date.day}/{date.year}"

def avalanche_problems_to_series(avalanche_problems):
    '''converts the avalanche_problems dictionary into a single series'''
    problems = pd.Series(dtype='object')
    for problem in list(avalanche_problems):
        for field in list(avalanche_problems[problem]):
            column_name = f"{problem}:{field}"
            problems[column_name]=avalanche_problems[problem][field]
    return problems

def read_avalanche_problems(forecast_url):
    '''collects the type, location, likelihood, size, and description of each avalanche problem'''
    page = requests.get(forecast_url)
    soup = BeautifulSoup(page.content, 'html.parser')
    problems = soup.findAll(class_='text_01 mb0')
    avalanche_problems = {}
    for problem in problems:
        problem_type = problem.next_sibling.nextSibling.string
        #print(problem.string, problem_type)
        problem_info_soup = problem.parent.parent.nextSibling.nextSibling
        
        problem_info = read_problem_fields(problem_info_soup) #dictionary of problem information
        problem_info['Type'] = problem_type
        avalanche_problems[problem.string] = problem_info
        
    return avalanche_problems

def read_problem_fields(problem_soup):
    '''returns a dictionary of information contained in an forcast's avalanche problem'''
    problem_info = {}
    PROBLEM_FIELDS = ['Location', 'Likelihood', 'Size', 'Description']
    for field in PROBLEM_FIELDS:
        if field == 'Description':
            # get the description and insert into problem_info
            description = problem_soup.find(string=field).parent.next_sibling.next_sibling.contents[1].string
            #print('Description:',description)
            problem_info[field] = description
        else:
            field_tag = problem_soup.find(string=field).parent
            #print(field_tag)
            img_tag = field_tag.next_sibling.next_sibling
            img_url = AVY_URL + img_tag.get('src')
            #print(img_url)
            problem_info[field] = get_field_info(img_url, field)
            if field == 'Location':
                problem_info['Location Image Url'] = img_url
    return problem_info

def get_field_info(img_url, field):
    '''extracts field information from the image linked to the img_url. Field information is either location rose data, 
    likelihood, or size'''
    if field == 'Location':
        #get Location Rose
        field_info = get_location_rose(img_url) #dictionary of aspect and elevations with risk levels
    elif field == 'Likelihood':
        #read likelihood figure
        field_info = measure_likelihood(img_url) #Unlikely, Likely, Certain (1-5)
    else:
        #read size figure
        field_info = measure_size(img_url) # Small - Medium - Large (1-5)
    return field_info

#### Danger rose
def classify_danger(rgb_tuple):
    '''takes an rgb_tuple and returns the danger rating of that color based on the minimum manhattan distance
    of the rgb value from the specific danger values linked to specific colors. 
    1-> Low -> Green, 2-> Moderate -> Yellow, 3 -> Considerable -> Orange, 4 -> High -> Red, 5-> Extreme -> Black
    eg. rgb_tuple = (5,255,6)-->'Green'-->'Low.'''

    colors = {1 : (0,255,0),
              2 : (255, 255, 0),
              3 : (255, 128, 0),
              4 : (255, 0, 0),
              5 : (0, 0,0),
              }

    manhattan = lambda x,y : abs(x[0] - y[0]) + abs(x[1] - y[1]) + abs(x[2] - y[2]) #uses manhatten distance
    distances = {k: manhattan(v, rgb_tuple) for k, v in colors.items()}
    danger = min(distances, key=distances.get)
    return danger

def get_rose_url(forecast_url):
    page = requests.get(forecast_url)
    soup = BeautifulSoup(page.content, 'html.parser')
    rose_url = AVY_URL + soup.find("img", class_="full-width compass-width sm-pb3").get('src')
    return rose_url

def get_danger_rose(forecast_url, plot = False):
    '''identifies the avalanche danger at each aspect and elevation of the danger rose and returns a dictionary
    of the results'''
    #coordinates for each elevation and aspect
    #elevations are Low (<8000), Mid(8-95000), and High (>9500)
    #(Aspect, Elevation) = (x,y)
    rose_coord = {('N', 'High'): (200, 130),
                  ('NE', 'High') : (225, 135),
                  ('NW', 'High') : (175, 135),
                  ('W', 'High') : (165, 155),
                  ('E', 'High') : (235, 155),
                  ('SW', 'High') : (175, 175), 
                  ('S', 'High'): (200, 185), 
                  ('SE', 'High'): (225, 175), 
                  ('N', 'Mid'):(200, 100),
                  ('NE', 'Mid'):(250, 115),
                  ('NW', 'Mid'): (150, 115),
                  ('W', 'Mid'):(125, 165),
                  ('E', 'Mid'):(275, 165),
                  ('SW', 'Mid'):(145, 215), 
                  ('S', 'Mid') : (200, 230),
                  ('SE', 'Mid'):(255, 215),
                  ('N', 'Low'):(200, 65),
                  ('NE', 'Low'):(280, 100),
                  ('NW', 'Low'):(120, 100),
                  ('W', 'Low'):(75, 170),
                  ('E', 'Low'):(325, 170),
                  ('SW', 'Low'):(110, 250), 
                  ('S', 'Low'):(200, 280), 
                  ('SE', 'Low'):(290, 250)}

    try:
        rose_url = get_rose_url(forecast_url)
    except:
        return None, None
    rose_img = Image.open(requests.get(rose_url, stream=True).raw)
    if plot:
        plt.imshow(rose_img)
        plt.axis('off')
    rose_danger = {}
    pix = rose_img.load()
    for region in rose_coord:
        x,y = rose_coord[region]
        if plot:
            plt.plot(x, y, 'k', marker = 'o', markersize=6)
        rose_danger[region] = classify_danger(pix[x,y][0:3])
        #print(region, 'the danger is', rose_danger[region])
    if plot:
        plt.show()
    return rose_danger, rose_url

###Location, Likelihood, and Size Figures

def get_location_rose(img_url, plot = False):
    '''identifies the avalanche problem locations at each aspect and elevation of the location rose specified by img_url 
    and returns a dictionary of the results'''
    #coordinates for each elevation and aspect
    #elevations are Low (<8000), Mid(8-95000), and High (>9500)
    #(Aspect, Elevation) = (x,y), #down->15, right-> 30
    rose_coord = {('N', 'High'): (200, 130),
                  ('NE', 'High') : (225, 135),
                  ('NW', 'High') : (175, 135),
                  ('W', 'High') : (165, 155),
                  ('E', 'High') : (235, 155),
                  ('SW', 'High') : (175, 175), 
                  ('S', 'High'): (200, 185), 
                  ('SE', 'High'): (225, 175), 
                  ('N', 'Mid'):(200, 100),
                  ('NE', 'Mid'):(250, 115),
                  ('NW', 'Mid'): (150, 115),
                  ('W', 'Mid'):(125, 165),
                  ('E', 'Mid'):(275, 165),
                  ('SW', 'Mid'):(145, 215), 
                  ('S', 'Mid') : (200, 230),
                  ('SE', 'Mid'):(255, 215),
                  ('N', 'Low'):(200, 65),
                  ('NE', 'Low'):(280, 100),
                  ('NW', 'Low'):(120, 100),
                  ('W', 'Low'):(75, 170),
                  ('E', 'Low'):(325, 170),
                  ('SW', 'Low'):(110, 250), 
                  ('S', 'Low'):(200, 280), 
                  ('SE', 'Low'):(290, 250)}
    
    #transform for location rose
    for location in list(rose_coord):
        coord = rose_coord[location]
        transformed_coord = (coord[0] + 30, coord[1] + 10)
        rose_coord[location] = transformed_coord
        
    rose_img = Image.open(requests.get(img_url, stream=True).raw)
    if plot:
        plt.imshow(rose_img)
        plt.axis('off')
    location_rose = {}
    pix = rose_img.load()
    for region in rose_coord:
        x,y = rose_coord[region]
        if plot:
            plt.plot(x, y, 'k', marker = 'o', markersize=6)
        location_rose[region] = classify_presence(pix[x,y][0:3])
        #print(region, 'the danger is', rose_danger[region])
    if plot:
        plt.show()
    return location_rose

def classify_presence(rgb_tuple):
    '''Similar to classify_danger function
    Present(blue) and Not Present(grey) categories indicate location, likelihood, or size of avy problem'''

    colors = {
              "Present" : (102, 178, 255),
              "Not Present" : (192, 192, 192)
              }

    manhattan = lambda x,y : abs(x[0] - y[0]) + abs(x[1] - y[1]) + abs(x[2] - y[2]) #uses manhatten distance
    distances = {k: manhattan(v, rgb_tuple) for k, v in colors.items()}
    location = min(distances, key=distances.get)
    return location

def measure_likelihood(img_url, plot = False):
    #coordinates of each certainty category
    #Certainty, certainty_factor = (x,y)
    scale_coord = {('Certain', 5): (35, 9),
                   ('Very Likely', 4) : (35, 68),
                  ('Likely', 3) : (35, 125),
                  ('Somewhat Likely', 2) : (35, 183),
                  ('Unlikely', 1) : (35, 241),
                  }


    scale_reading = {}
    img_likelihood = Image.open(requests.get(img_url, stream=True).raw)
    if plot:
        plt.imshow(img_likelihood)
        plt.axis('off')
    pix = img_likelihood.load()
    for likelihood in scale_coord:
        x,y = scale_coord[likelihood]
        if plot:
            plt.plot(x, y, 'k', marker = 'o', markersize=6)
        scale_reading[likelihood] = classify_presence(pix[x,y][0:3])
        #print(likelihood, 'coordinates are', scale_coord[likelihood], 'and which reads as',scale_reading[likelihood])
        if scale_reading[likelihood] == 'Present':
            likelihood, likelihood_factor = likelihood
            return likelihood, likelihood_factor
    return None

def measure_size(img_url, plot = False):
    #coordinates of each certainty category
    #Certainty, certainty_factor = (x,y)
    scale_coord = {('Large', 5): (35, 9),
                   ('Medium-Large', 4) : (35, 68),
                  ('Medium', 3) : (35, 125),
                  ('Medium-Small', 2) : (35, 183),
                  ('Small', 1) : (35, 241),
                  }


    scale_reading = {}
    img_size = Image.open(requests.get(img_url, stream=True).raw)
    if plot:
        plt.imshow(img_size)
        plt.axis('off')
    pix = img_size.load()
    for size in scale_coord:
        x,y = scale_coord[size]
        if plot:
            plt.plot(x, y, 'k', marker = 'o', markersize=6)
        scale_reading[size] = classify_presence(pix[x,y][0:3])
        #print(size, 'coordinates are', scale_coord[size], 'and which reads as',scale_reading[size])
        if scale_reading[size] == 'Present':
            size, size_factor = size
            return size, size_factor
    return None

def get_page_forecasts(url):
    '''returns a dataframe of forecasts from url. Data in df
    includes Date, Region, Forecast Title, (url) extension, and
    forecastor'''
    #get raw talbe
    page_forecasts = get_html_table(url)
    #Prep for relabeling
    old_columns = page_forecasts.columns
    r = re.compile(r"Forecast:\s(.*?)\sArea\sMountains") # regex for finding region
    page_forecasts['Region'] = page_forecasts.iloc[:,1].apply(lambda x: r.match(x[0]).group(1))
    # change names
    page_forecasts[['Date', 'a']] = pd.DataFrame(page_forecasts[old_columns[0]].tolist(), index=page_forecasts.index)
    page_forecasts[['Observation Title', 'extension']]= pd.DataFrame(page_forecasts[old_columns[1]].tolist(), index=page_forecasts.index)
    page_forecasts[['Forecaster', 'b']]= pd.DataFrame(page_forecasts[old_columns[2]].tolist(), index=page_forecasts.index)
    #remove old columns and columns with None
    page_forecasts=page_forecasts.drop(list(old_columns) + ['a', 'b'], axis = 1)
    return page_forecasts

################ Storage ############################
def save_data(df, data_type = 'avalanche', filename=None, timed=False):
    '''saves df to a .csv file, data_type is one of ['avalanche', 'forecast', 'observation']'''
    time_start=time()
    if filename:
        df.to_csv(filename, index=False)
        print("Saved data to", filename)
        return
    elif data_type == 'forecast':
        start_date = df['Date'].min()
        end_date = df['Date'].max()
        #insert region
    else:
        start_date = df['Observation Date'].min()
        end_date = df['Observation Date'].max()
    
    #print(start_date)
    start_date=str(start_date).replace('-','_')
    end_date = str(end_date).replace('-', '_')
    if data_type == 'forecast':
        region = df['Region'].iloc[0]
        filename = f"{data_type}@{region}@{start_date}&{end_date}.csv"
    else:
        filename = f"{data_type}@{start_date}&{end_date}.csv"
    
    df.to_csv(filename, index=False)
    time_stop = time()
    if timed:
        print("Time elapsed for save:", (time_stop-time_start)/60, 's.')
    return

def load_data(filename):
    return pd.read_csv(filename, index_col=False)

def update_data(filename, timed = False):
    '''updates data stored in filename with new information'''
    time_start = time()
    data_type = filename.split('@')[0]
    region = ''
    print(filename, data_type)
    if data_type == './forecast':
        region = filename.split('@')[1]
    start_date = filename.split('@')[-1].split('&')[0]
    end_date = filename.split('@')[-1].split('&')[1].split('.')[0]
    
    old_data = load_data(filename)
    end_date = datetime.datetime.strptime(end_date, '%Y_%m_%d').date() + datetime.timedelta(days=1)
    if end_date == TODAY:
        return old_data
    
    if data_type == './forecast':
        new_data = get_forecasts(start_date=end_date , end_date=TODAY, region=region)
    elif data_type == './avalanche':
        new_data, err = get_avalanche_data(start_date=end_date, end_date=TODAY)
    else:
        new_data, err = get_observation_data(start_date=end_date, end_date=TODAY)
    
    updated_data = pd.concat([old_data, new_data], axis=0)
    end_date = str(TODAY).replace('-','_')
    
    if data_type == 'forecast':
        filename = f"{data_type}@{region}@{start_date}&{end_date}.csv"
    else:
        filename = f"{data_type}@{start_date}&{end_date}.csv"
    
    save_data(updated_data, data_type=data_type, filename=filename)
    #fix index loading
    time_stop = time()
    if timed:
        print("Time elapsed for update:", (time_stop-time_start)/60, 's.')
    return updated_data

def main():
    print('~~ Welcome to the Utah Avalanche Data Scrape ~~') 
    #start_date = TODAY - datetime.timedelta(days = 14) #test for last two weeks
    start_date = get_season_start(TODAY)
    print('Looking for observations between', TODAY, 'and', start_date)
    #obs_table =  get_observation_table(start_date=start_date)
    #print('Found', len(obs_table), 'observations.')
    #while debugging its good to have smaller data set
    #if len(obs_table) > 500:
    #    print('Too many observations to scan right now...closing application.')
    #    return
    #print(obs_table.head())
    
    print('Scraping avalanche data...\n')
    avalanches, err = get_avalanche_data(start_date=start_date, end_date = TODAY, timed=True)
    print(avalanches.head())
    save_data(avalanches, data_type = 'avalanche')
    
    print('Scraping general observation data...\n')
    observations, err = get_observation_data(start_date=start_date, end_date=TODAY, verbose=False, timed=True)
    print(observations.head())
    save_data(observations, data_type = 'observations')

    print('Scraping forecasts for Salt Lake...\n')
    forecasts = get_forecasts(start_date= start_date, end_date=TODAY,region= 'Salt Lake', timed=True)
    print(forecasts.sort_values('Date',ascending=False).reset_index(drop=True))
    print(forecasts.head())
    save_data(forecasts, data_type='forecast')

    #print('Today\'s danger rose reads as...\n')
    #danger_rose= forecasts.loc[14]["Danger Rose"] 
    #print(danger_rose)
    return avalanches, observations, forecasts

def get_season_observations():
	'''collects all the observations and stores them in .csv files'''
	return None

if __name__ == '__main__':
    main()
