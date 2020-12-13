#!/usr/bin/env python
# coding: utf-8

# In[ ]:

import os
os.system('pip install wget')
import wget 
import zipfile
import sys
import stat
from bs4 import BeautifulSoup
from selenium import webdriver
import pandas as pd
import datetime

def get_date():
    # Data set 2 is for investigating the relationship between changes of popularity of fast food and Covid-19 growth
    # Health Department of California started to publish Covid-19 data on 03/18/2020, so we will get popularity data 
    # from that date. 
    start_time = datetime.datetime(2020, 3, 18)
    return start_time.date()

def get_urls_from_source1(dataset,county_name):
    """returns a list of urls to scrape
    dataset pandas dataframe
    county_name str"""
    # to get data source 2, we need to utilize data in source 1, dataset1 here.
    # by summarizing counties' total popularity values, we figured out the county where fast food has the most popularity.
    # then, we'll scrape reviews' data since March 2020 from pages of businesses in that county
    filters = [dataset.index[i][0] == county_name for i in range(len(dataset))]
    urls = dataset[filters]['url'].values.tolist()
    print(urls)
    return urls

def set_up_scraper():
    # download the plug-in corresponding to operating system, in order to make the webdriver work
    sys_name = sys.platform
    current_directory = os.getcwd()
    if sys_name == 'win32':
        file_url = 'https://chromedriver.storage.googleapis.com/87.0.4280.20/chromedriver_win32.zip'
    elif sys_name == 'darwin':
        file_url = 'https://chromedriver.storage.googleapis.com/87.0.4280.20/chromedriver_mac64.zip'
    else:
        file_url = 'https://chromedriver.storage.googleapis.com/87.0.4280.20/chromedriver_linux64.zip'
    file = wget.download(file_url,out=current_directory)
    with zipfile.ZipFile(file,'r') as zip_ref:
        zip_ref.extractall('chromedriver')
    # make the plug-in work
    st = os.stat("./chromedriver/chromedriver")
    os.chmod("./chromedriver/chromedriver", st.st_mode | stat.S_IEXEC)
    options = webdriver.ChromeOptions()
    options.add_argument('--headless')
    # start the driver
    driver = webdriver.Chrome(executable_path=current_directory+'/chromedriver/chromedriver',options=options)
    return driver
    

def get_soup(driver,url):
    driver.get(url)
    soup = BeautifulSoup(driver.page_source, 'lxml')
    return soup

def business_page_data(grade_state, county_name, dataset_one):
    """retrieve review date and rating from businesses' pages,
    and returns a data frame containing this data.
    grade_state boolean 
    county_name str
    dataset_one pandas dataframe
    """
    # firstly set up a good crawling environment 
    driver = set_up_scraper()
    # count amount of requesting calls for realizing the grade mode
    count = 1
    start_date = get_date()
    list_of_webpages = get_urls_from_source1(dataset_one, county_name)
    # store reviews' information
    list_of_reviews = []
    # a reminder to tell users that this program is still working, under remote mode
    if not grade_state:
        print(' Almost there! Source 2 crawler needs few minutes to get data...')
    # traverse the list of webpages
    for url in list_of_webpages:
        current_page = 1
        # get soup and number of total pages
        soup = get_soup(driver, f'{url}&sort_by=date_desc') # make reviews in the newest first order
        total_page = int(soup.find('span',{'class':'lemon--span__373c0__3997G text__373c0__2Kxyz text-color--black-extra-light__373c0__2OyzO text-align--left__373c0__2XGa-'}).text[-1])
        while True:
            reviews = soup.find_all('li',{'class': 'lemon--li__373c0__1r9wz margin-b3__373c0__q1DuY padding-b3__373c0__342DA border--bottom__373c0__3qNtD border-color--default__373c0__3-ifU'})
            for review in reviews:
                date = review.find('span',{'class':'lemon--span__373c0__3997G text__373c0__2Kxyz text-color--mid__373c0__jCeOG text-align--left__373c0__2XGa-'})
                review_date = datetime.datetime.strptime(date.text, '%m/%d/%Y').date()
                # if found some reviews earlier than start date, then stop scraping and move to next url
                if review_date < start_date:
                    break
                rating = review.find('div',{'role':'img'}).attrs['aria-label']
                list_of_reviews.append((review_date,int(rating[0])))
            # follow the 'max 3' rule under grade mode
            if grade_state and count == 3:
                break
            # go to the next page
            current_page += 1
            if current_page <= total_page:
                # have a url for page 2 or more, similarly make reviews listed in newest first order
                new_url = f'{url}&start={(current_page-1)*20}&sort_by=date_desc'
                soup = get_soup(driver,new_url)
                count += 1
            # if we've read the last page, then move to the next business' url
            else:
                break
        # samely follow the 'max 3' rule under grade mode    
        if grade_state and count == 3:
            break
    cols = ['review_date', 'rating']
    # create a data frame based on the list of reviews' information, and name the columns 
    df = pd.DataFrame.from_records(list_of_reviews,columns=cols)
    print('Successfully get data from source 2!')
    return df

def calculate_daily_popularity(df):
    """calculate daily popularity of fast food from reviews' information
    df pandas dataframe"""
    # formula is same as the one used on raw data from source 1
    total_ratings = df['total_rating'].sum()
    rating_count = df['times'].sum()
    # R3 equals:
    global_average_rating = total_ratings / rating_count
    # get the max of review count, N value
    max_review_ct = df['times'].max()
    # get the W, weight value
    weight = df['times']/max_review_ct
    # add the popularity of each business into the data frame
    popularity = weight * df['average_rating'] + (1-weight) * global_average_rating
    df['popularity'] = popularity
    return df

def process_webpages_data(df):
    # get each date's occurence, daily total rating, daily avergae rating, and finally daily popularity
    review_count = df.groupby('review_date').agg('count')
    # give names to columns
    review_count.rename(columns={'rating':'times'},inplace=True)
    review_count['total_rating'] = df.groupby('review_date')['rating'].sum()
    review_count['average_rating'] = review_count.total_rating/review_count.times
    print('Successfully create new features for data set 2!')
    return calculate_daily_popularity(review_count)
    
    

