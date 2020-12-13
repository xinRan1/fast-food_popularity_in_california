#!/usr/bin/env python
# coding: utf-8

# In[1]:


import requests as r
import datetime
import pandas as pd

def read_covid_records(records, list_covid_records):
    """extract county name, new cases, and new deaths information 
    from a list in json.
    records list
    list_covid_records list"""
    for record in records:
        date = datetime.datetime.strptime(record['date'], '%Y-%m-%dT%H:%M:%S').date()
        county_time_case_death = (record['county'],date,record['newcountconfirmed'],record['newcountdeaths'])
        list_covid_records.append(county_time_case_death)

def get_records_json(url):
    """get data from url in json format
    url str"""
    # try to get data from url 
    try:
        respond = r.get(url)
        respond.raise_for_status()
    # otherwise show an error and returns none
    except r.exceptions.HTTPError as e:
        print(e)
        return None
    else:
        json_recorder = respond.json()
        respond.close()
    return json_recorder
    
def covid_records(grade_state): 
    """retrieve data of Covid-19 updates in California since March 2020"""
    list_covid_records = []
    start_url = 'https://data.ca.gov/api/3/action/datastore_search?resource_id=926fd08f-cc91-4828-af38-bd45de97f8c3'
    records_json = get_records_json(start_url)
    # count calls to the API for realizing grade mode
    count = 1
    # make sure the first page of API can be downloaded 
    if records_json != None:
        # find the records list from json
        covid_records_results = records_json['result']
        covid_records = covid_records_results['records']
        while covid_records != []:
            read_covid_records(covid_records, list_covid_records)
            # stop crawling when we reached the third call under grade mode
            if grade_state and count == 3:
                break
            # go to the next url 
            next_url = 'https://data.ca.gov'+ covid_records_results['_links']['next']
            records_json = get_records_json(next_url)
            count += 1
            if records_json != None:
                covid_records_results = records_json['result']
                covid_records = covid_records_results['records']
            # when the url can't be downloaded, stop calling 
            else:
                break
    # create a dataframe based on data retrieved from the API            
    cols = ['county','date','new_cases', 'new_deaths'] 
    covid_records_df = pd.DataFrame.from_records(list_covid_records,columns=cols)
    print('Successfully get data from source 4!')
    return covid_records_df
    

    
    
    
    

