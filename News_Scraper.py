#!/usr/bin/env python
# coding: utf-8

# In[42]:


import requests as r
from bs4 import BeautifulSoup
import datetime
import pandas as pd

def request_url (url):
    # make sure downloading the web page successfully 
    try:
        respond = r.get(url)
        respond.raise_for_status()
    except r.exceptions.HTTPError as e:
        print(e)
        return None
    else:
        soup = BeautifulSoup(respond.content, 'html.parser')
        respond.close()
    return soup
    

def get_date():
    # with data set 2, I'm investigating the relationship between Covid-19 growth and changes of popularity of fast food 
    # in California at the same peride. 
    # Health Department of California started to publish Covid-19 data on 06/26/2020, so we will get popularity data 
    # after that date. 
    start_time = datetime.datetime(2020, 3, 18)
    return start_time.date()

def count_news_about_fastfood(grade_state):
    fast_food_url = 'https://www.dailynews.com/?s=fast+food&orderby=relevance&order=asc&date=2019-11-28&sp%5Bf%5D=2019-11-28&sp%5Bt%5D=2020-11-28&post_type='
    # count variable helps control calls in the grade mode
    if grade_state:
        count = 0
    else:
        count = 4
    # focus on news from the start date
    start_date = get_date()
    date_n_article_number = dict()
    contents = request_url(fast_food_url)
    if contents != None:
        while True:
            count += 1
            articles_info = contents.find_all('div',{'class': 'article-info'})
            for article in articles_info:
                date_str = article.find('time').attrs['datetime'][:10]
                # get the news' date of being posted
                date = datetime.datetime.strptime(date_str, '%Y-%m-%d').date()
                # only count those posted after 3/18/2020
                if date >= start_date:
                    date_n_article_number[date] = date_n_article_number.get(date, 0) + 1
            next_page = contents.find('a',{'class':'load-more'})
            if next_page != None and count != 3:
                # go to the next page if possible 
                contents = request_url(next_page.attrs['href'])
            else:
                break
    return date_n_article_number

def get_fast_food_related_news_count(grade_state):
    # the dictionary counting each day's news quantity 
    data_dict = count_news_about_fastfood(grade_state)
    col_name = ['number_of_fast_food_related_news']
    # create a data frame based on the dictionary and name columns
    news_count_df = pd.DataFrame.from_dict(data_dict, orient='index', columns = col_name)
    news_count_df.index.name = 'date'
    print('Successfully get data from source 3!')
    return news_count_df
                
            
            
            
    

