#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import Yelp_Fusion_API_crawler as Cali_popularity 
import Yelp_recent_reviews_scraper as County_reviews
import News_Scraper as popularity_from_news
import CA_Covid_API as Cali_covid_records
import pandas as pd
import argparse 
   
def crawlers_1_3_4(grade_state):
    # get raw data from Yelp Fusion API, News webpages, and California Covid-19 API
    # when the grade state is true, we follow the 'max 3' rules, otherwise crawl entire data set
    Cali_fast_food_popularity = Cali_popularity.businesses_info(grade_state)
    Cali_news = popularity_from_news.get_fast_food_related_news_count(grade_state)
    Covid_records = Cali_covid_records.covid_records(grade_state)
    return [Cali_fast_food_popularity, Cali_news, Covid_records]


def feature_generation_on_1(raw_data):
    """first data pre-process; add the popularity feature into raw data set 1.
    raw data list
    """
    raw_df = raw_data[0]
    # because popularity's calculation is dependent on the variance of entire data set, the popularity values 
    # will be different under grade mode and remote mode, but the calculation works normally with any data input.
    # in the main presentation of the project, input data will show information up to the submission date, so the
    # final results of analysis will be fixed.
    processed_df = Cali_popularity.businesses_info_popularity(raw_df)
    # returns the new list with the processed data set 1
    return [processed_df,raw_data[1],raw_data[2]]

def crawler_2(grade_state, data):
    """work dependently on url list and a key word gotten from data set 1; 
    returns a list with all 4 data sets.
    grade_state boolean
    data list
    """
    # get the key word, name of the county with most fast-food popularity in California firstly
    objective_county = Cali_popularity.county_with_most_popularity(data[0])
    # crawl urls in the second source
    print(objective_count)
    County_recent_reviews = County_reviews.business_page_data(grade_state, objective_county, data[0])
    return [data[0],County_recent_reviews,data[1],data[2]]

def feature_generation_on_2(data):
    """second data pre-processing; generate 3 new features into raw data set 2,
    returns a list with all 4 data sets after pre-process.
    data list
    """
    raw_reviews = data[1]
    # for the same reason as in the first process, the popularity values 
    # will be different under grade mode and remote mode.
    processed_reviews = County_reviews.process_webpages_data(raw_reviews)
    return [data[0],processed_reviews,data[2],data[3]]

def store_datasets (preprocessed_data):
    '''save pre-processed data sets to csv files
    preprocessed_data list
    '''
    preprocessed_data[0].to_csv("Cali_fastfood_popularity.csv", index=True, sep=',')
    preprocessed_data[1].to_csv("recent_fast_food_reviews.csv", index=True, sep=',')
    preprocessed_data[2].to_csv("News_info.csv", index=True, sep=',')    
    preprocessed_data[3].to_csv("Covid_19_records.csv", index=True, sep=',')
    print('Pre-processed data sets are successfully stored as csv files!')
    
def get_and_process_data(grade_state):
    """crawl and pre-process all data sets.
    grade_state boolean
    """
    raw_data_1_3_4 = crawlers_1_3_4(grade_state)
    first_process = feature_generation_on_1(raw_data_1_3_4)
    dataset_1_2_3_4 = crawler_2(grade_state, first_process)
    data_after_second_process = feature_generation_on_2(dataset_1_2_3_4)
    return data_after_second_process
        
def get_data_from_local_files():
    """read csv files from disk, save data as panda data frames, 
    and returns a list of data sets.
    """
    Cali_fast_food_popularity = pd.read_csv("Cali_fastfood_popularity.csv", index_col = 0)
    County_recent_reviews = pd.read_csv("recent_fast_food_reviews.csv", index_col = 0)
    Cali_news = pd.read_csv("News_info.csv", index_col = 0)
    Covid_records = pd.read_csv("Covid_19_records.csv", index_col = 0)
    return [Cali_fast_food_popularity, County_recent_reviews, Cali_news, Covid_records]
        
def main():
    parser = argparse.ArgumentParser(description='Get data for the final project')
    # set up three options for data sources
    parser.add_argument('--source', choices=['remote','local','grade'], 
                        required=True, type=str, help='please choose the data source')
    args = parser.parse_args()
    data_src = args.source
    if data_src == "local":
        data = get_data_from_local_files()
        print('Successfully get data from local sources!')
    elif data_src == 'remote':
        print('Please be patient. It may take some time for crawlers to get data:)')
        data = get_and_process_data(False)
    else:
        # under the grade mode, data sets will contain data gotten by first three calls for source 3 and 4;
        # for source 1, we call the API with locations, Orange county, LA county, and Santa Barbara county,
        # to guarantee that there are proper recent reviews in fast-food businesses' web pages. This makes data set
        # from source 2 non-empty.
        data = get_and_process_data(True)
    store_datasets (data)
   

if __name__ == '__main__':
    main()

