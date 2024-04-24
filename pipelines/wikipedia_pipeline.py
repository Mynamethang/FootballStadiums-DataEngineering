import json
import pandas as pd
from bs4 import BeautifulSoup
import requests
from datetime import datetime
import os
from sqlalchemy import create_engine
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderUnavailable
from time import sleep


NO_IMAGE = 'https://upload.wikimedia.org/wikipedia/commons/thumb/0/0a/No-image-available.png/480px-No-image-available.png'

def extract_wikipedia_data(**context):
    #request url 
    url = context['url']
    print("Getting wikipedia page...", url)
    try:
        response = requests.get(url)
        response.raise_for_status()# check if the request is successful
    except requests.RequestException as e:
        print(e)

    soup = BeautifulSoup(response.text, 'html.parser')
    #extract tables
    tables = soup.find_all('table')
    find_table = soup.find('table',  class_="wikitable sortable sticky-header")

    tr_tags = find_table.find_all('tr')
    #exrtact column name
    th_tags = tr_tags[0].find_all('th')
    data_columns = [th.text.strip('\n')  for th in th_tags]

    #extract row value
    data_rows = []
    for i in range(1, len(tr_tags)):
        td_tags = tr_tags[i].find_all('td')
        #0-stadium name
        #1-seating capacity
        #2-region
        #3-contruy
        #4-city
        #5-image
        #6-hometeam
        data_rows.append([
            i,
            td_tags[0].text.replace(' ♦', '').strip('\n'),
            td_tags[1].text.strip('\n').split('[')[0].replace(',', '').replace('.', ''),
            td_tags[2].text.strip('\n'),
            td_tags[3].text.replace('\xa0', '').strip('\n'),
            td_tags[4].text.strip('\n'),
            td_tags[5].find('img').attrs['src'] if td_tags[5].find('img') else None,
            td_tags[6].text.strip('\n')
        ])

    context['ti'].xcom_push(key='columns', value=data_columns)
    context['ti'].xcom_push(key='rows', value=data_rows)

    return f'Push completely! + column name: {data_columns}'

def get_coordinates(city, country):
    coordinates_cache = {}
    retries=3
    delay=2
    
    location_key = f"{city}, {country}"
    if location_key in coordinates_cache:
        return coordinates_cache[location_key]
    else:
        for _ in range(retries):
            try:
                geolocator = Nominatim(user_agent="geo_locator")
                location = geolocator.geocode(location_key, timeout=10)  # Increase timeout if needed
                if location:
                    coordinates = f"{location.latitude}, {location.longitude}"
                    coordinates_cache[location_key] = coordinates
                    return coordinates
                else:
                    return None
            except GeocoderUnavailable:
                print("GeocoderUnavailable error. Retrying after delay...")
                sleep(delay)
        else:
            print("Max retries exceeded. Could not retrieve coordinates.")
            return None


def transform_wikipedia_data(**context):
    columns = context['ti'].xcom_pull(task_ids='extract_wikipedia_data', key='columns')
    rows = context['ti'].xcom_pull(task_ids='extract_wikipedia_data', key='rows')

    #format column name
    new_columns = [i.replace(' ', '_').lower() for i in columns]

    #create stadiums dataframe
    stadiums_df = pd.DataFrame(data=rows, columns=new_columns)

    #add coordinate column base on provided city and country values
    stadiums_df['coordinate'] = stadiums_df.apply(lambda row: get_coordinates(row['city'], row['country']), axis=1)
    #stadiums_df[['latitude', 'longitude']] = pd.DataFrame(stadiums_df['location'].tolist(), index=stadiums_df.index)

    #transform data
    stadiums_df.rename(columns={"stadium": "stadium_name"}, inplace=True)
    stadiums_df['images'] = stadiums_df['images'].apply(lambda x: x if x not in ['NO_IMAGE', '', None] else NO_IMAGE)
    stadiums_df['seating_capacity'] = stadiums_df['seating_capacity'].astype(int)

    context['ti'].xcom_push(key='stadiums_data', value=stadiums_df.to_json())

    return f'Push completely! + column name: {columns}'


def write_wikipedia_data(**context):
    
    data = context['ti'].xcom_pull(key='stadiums_data', task_ids='transform_wikipedia_data')

    data_json = json.loads(data)
    data_df = pd.DataFrame(data_json)

    #store transformed data as csv file
    file_name = ('stadium_cleaned_' + str(datetime.now().date()) + "_" + str(datetime.now().time()).replace(":", "_") + '.csv') #file name with timestamp
    data_df.to_csv(f'data/{file_name}' , index=False, mode='w')

    # data.to_csv('abfs://footballdataeng@footballdataeng.dfs.core.windows.net/data/' + file_name,
    #             storage_options={
    #                 'account_key': 'pcrbWAsuPmzOH43lu1xang05pIs+g1Lys/bor0z59O38sVyWQNQ64AtEveMobZ2pIwCjqximReKY+ASt9dP/+A=='
    #             }, index=False)

    return data_df
   

def load_to_database(**context):
    df = context['task_instance'].xcom_pull(task_ids='write_wikipedia_data')

    #insert data into postgresSQL
    engine = create_engine('postgresql://postgres:12345678@host.docker.internal:5432/mydb')

    df.to_sql('word_stadium', engine, if_exists='replace', index=False)

    return "Load to database successfully"
    