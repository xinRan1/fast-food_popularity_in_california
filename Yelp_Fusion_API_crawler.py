#!/usr/bin/env python
# coding: utf-8

# In[33]:


# This file is to crawl businesses' information from Yelp.com Fusion API
# Yelp Fusion API Documentation https://www.yelp.com/developers/documentation/v3/business_search

import requests as r
import pandas as pd

def get_counties_list():
    counties = 'Alameda County, California Alpine County, California Amador County, California Butte County, California Calaveras County, California Colusa County, California Contra Costa County, California Del Norte County, California El Dorado County, California Fresno County, California Glenn County, California Humboldt County, California Imperial County, California Inyo County, California Kern County, California Kings County, California Lake County, California Lassen County, California Los Angeles County, California Madera County, California Marin County, California Mariposa County, California Mendocino County, California Merced County, California Modoc County, California Mono County, California Monterey County, California Napa County, California Nevada County, California Orange County, California Placer County, California Plumas County, California Riverside County, California Sacramento County, California San Benito County, California San Bernardino County, California San Diego County, California San Francisco County, California San Joaquin County, California San Luis Obispo County, California San Mateo County, California Santa Barbara County, California Santa Clara County, California Santa Cruz County, California Shasta County, California Sierra County, California Siskiyou County, California Solano County, California Sonoma County, California Stanislaus County, California Sutter County, California Tehama County, California Trinity County, California Tulare County, California Tuolumne County, California Ventura County, California Yolo County, California Yuba County'
    return counties.split(', California ')

def get_response_json(url,key,location):
    # credential: header
    headers = {
        'Authorization' : 'Bearer %s' % key
    }
    parameters = {'term':'fast food','location':location}
    # gruarantee that webpage is successfully requested
    try:
        response = r.get(url,headers=headers,params=parameters)
        response.raise_for_status()
    except r.exceptions.HTTPError as e:
        print(e)
        return None
    else: 
        businesses_json = response.json()
        response.close()
    return businesses_json

def businesses_info(grade_state):
    # only call API for three counties under the grade mode
    if grade_state:
        counties_list = ['Orange County', 'Los Angeles County', 'Santa Barbara County'] 
    else:
    # get a string of all counties in California under the remote mode
        counties_list = get_counties_list()
    # credentials: the url, API Key
    url = 'https://api.yelp.com/v3/businesses/search'
    key = # enter your own key
    # a list recording businesses' information
    list_of_businesses = []
    for county in counties_list:      
        # unify the format of location
        location = county + ', CA'
        # try to retrieve information of fast food businesses in a county
        county_json = get_response_json(url,key,location)
        if county_json != None:
            for business in county_json['businesses']:
            # for each business, we form a tuple for its county, id, name, reviews' count, rating, transaction forms, url, and state 
                list_of_businesses.append((county,business['id'], business['name'],business['review_count'],business['rating'],', '.join(business['transactions']), business['url'],business['location']['state']))
    # names of columns in a data frame, which represent the information
    cols = ['county','business_id','business_name','review_count','rating','transaction_forms','url','state']
    # create the data frame with column names above
    df = pd.DataFrame.from_records(list_of_businesses,columns=cols)
    # clear all businesses which don't belong to the state of CA
    df = df[df.state == 'CA']
    # group the spreadsheet by county, and let business_id as the sub-index
    df.set_index(['county','business_id'],inplace=True)
    print('Successfully get data from source 1!')
    return df
    

def businesses_info_popularity(Yelp_Cali_df):
    """calculate the popularity of fast food businesses 
    and add it as a new column"""
    # reasoning behind my calculation of popularity:
    # R1 := popularity value (0-5) of a business; R2 := rating of the business; R3 := global average of ratings
    # W (weight factor) := n/N s.t. n := rating count = review count (every rating is affiliated to a review), and
    # N := max rating count = max review count
    # R1 = W*R2+(1-W)*R3
    
    # Let's get R3 firstly
    total_ratings = (Yelp_Cali_df['review_count']*Yelp_Cali_df['rating']).sum()
    rating_count = Yelp_Cali_df['review_count'].sum()
    # R3 equals:
    global_average_rating = total_ratings / rating_count
    # get the max of review count, N 
    max_review_ct = Yelp_Cali_df['review_count'].max()
    # get the W, weight value
    weight = Yelp_Cali_df['review_count'] / max_review_ct
    # add the popularity of each business(row) into the data frame
    popularity = weight * Yelp_Cali_df['rating'] + (1-weight) * global_average_rating
    Yelp_Cali_df['popularity'] = popularity
    print('Successfully get a new feature on source 1 data!')
    return Yelp_Cali_df


def county_with_most_popularity(df):
    """returns the name of county with the most fast-food popularity"""
    total_popularity_df = df.groupby('county')['popularity'].sum()
    total_popularity_df.sort_values(ascending=False,inplace=True)
    return total_popularity_df.index[0]
    

