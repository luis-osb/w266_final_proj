# -*- coding: utf-8 -*-
"""
Created on Thu Jul 15 00:39:13 2021

@author: luiso
"""

import requests
import os
import json

import csv
import datetime
import dateutil.parser
import unicodedata

import time

import pandas as pd


# To set your environment variables in your terminal run the following line:
# export 'BEARER_TOKEN'='<your_bearer_token>'
bearer_token = "AAAAAAAAAAAAAAAAAAAAACjCRgEAAAAAL7wCgpI0iKqAbf%2FmLSBCSz2UkzY%3D2tTqxioYRQ4SRH1ArAxsmNccjJ6btucdKrJXt0Q5hFA5U0pC09"

search_url = "https://api.twitter.com/2/tweets/search/recent"

acc_info = pd.read_csv('accounts_data.csv')

###################################################################

#example_qry = {'query': 'from:twitterdev (-is:retweet)',\
#                'start_time':'2021-07-13T05:04:00.000Z', \
#                'tweet.fields':'created_at', 'expansions' : 'author_id'}


example_qry = {'query': 'from:twitterdev (-is:retweet)',\
                'start_time':'2021-07-19T16:00:00.000Z', \
                'tweet.fields':'created_at', 'expansions' : 'author_id'}



#def gen_params(acct = 'twitterdev' , from_date = '2021-07-12T15:16:00.000Z',  test = 0):
def gen_params(acct = 'twitterdev' , from_date = '2021-07-19T16:00:00.000Z',  test = 0):
     
    if test == 1:
        return example_qry
            
    else:
    
        query_txt = 'from:' + acct + '(-is:retweet)'
        
        return {'query': query_txt , 'start_time': from_date,\
                'tweet.fields':'created_at', 'expansions' : 'author_id'}



def bearer_oauth(r):
    """
    Method required by bearer token authentication.
    """

    r.headers["Authorization"] = f"Bearer {bearer_token}"
    r.headers["User-Agent"] = "v2RecentSearchPython"
    return r

def connect_to_endpoint(url, params):
    response = requests.get(url, auth=bearer_oauth, params=params)
    print(response.status_code)
    if response.status_code != 200:
        raise Exception(response.status_code, response.text)
    return response.json()



#####################################################################

os.chdir(r'C:\Users\luiso\Desktop\Bidness Stuff\MIDS\w266_finproj')

acc_info = acc_info[acc_info.has_twitter == 1]
acc_info['twitter'] = acc_info['twitter'].str.replace("@|'", "", regex = True) 


### TO DO: 
#1.TWEAK GEN_PARAM TO GET TWEET TIMESTAMP
#2.VER ESSA DOS ACENTOS/UNICODE - N QUERO QUE SAIA CAGADO
#3.EARLY LEMMATIZATION:CONSIDER KEEPING RAW STRINGS BUT :
    #HAving a single character for links
    #splitting hashtags from their actual words, e.g. #educacao --> #, educacao
    #but maybe not yet - the og paper doesn't do that

final_df = []
request_count = 0
for index, row in acc_info.iterrows():
    username = row['twitter']
    #print(username)
    tweets = connect_to_endpoint(search_url, gen_params(username, test = 0))    
    request_count +=1
    #print(tweets)
    
    if tweets['meta']['result_count'] > 0:
        for tweet in tweets['data']:
            
            new_row = row.tolist()
            new_row.append(tweet['created_at'])
            new_row.append(tweet['author_id'])
            new_row.append(tweet['id'])
            new_row.append(tweet['text'])
            print(new_row)

            
            final_df.append(new_row)
    
    if request_count == 280:
        print("Time for a lil sleep!")
        time.sleep(60*16)
        request_count = 0


final_df_cols = list(acc_info.columns) + ['created_at', 'author_id', 'tweet_id', 'tweet_text']


final_df_pd = pd.DataFrame(final_df, columns = final_df_cols)

final_df_pd.to_csv('7_19_data_subset_rety.csv')



# Optional params: start_time,end_time,since_id,until_id,max_results,next_token,
# expansions,tweet.fields,media.fields,poll.fields,place.fields,user.fields

#Params I need: 
#1.is: tweet(fixed)
#2.from: a bunch of accounts
#3. since: oct. 7 2018







#json_response = connect_to_endpoint(search_url, query_params)
json_response = connect_to_endpoint(search_url, gen_params(test = 0))
print(json.dumps(json_response, indent=4, sort_keys=True))
