# -*- coding: utf-8 -*-
"""
Created on Thu Jul 29 20:37:27 2021

@author: luiso
"""

# -*- coding: utf-8 -*-
"""
Created on Thu Jul 15 00:39:13 2021

@author: luiso
"""

import requests
import os
import json

import csv
from datetime import datetime, date
import dateutil.parser
import unicodedata

import time

import pandas as pd

from dateutil.relativedelta import relativedelta


api_key = 's2e1qW7UaunQ94L5bJ2ueBDIz'
api_sec_key = 'vKI2MqGoKRpI0U48fT1eoqMnUusLfKJKbOLDVnGEzP8O4HkLtg'



# To set your environment variables in your terminal run the following line:
# export 'BEARER_TOKEN'='<your_bearer_token>'
bearer_token = "AAAAAAAAAAAAAAAAAAAAABm3RwEAAAAAQN9oSxZH1y1kJC9l41fPwRlqtb0%3DC56M960dos6MUzJFtn6jKpZP6UPIBeta07jAxAzqbxb6ZeYCM7"

search_url = "https://api.twitter.com/2/tweets/search/all"

def bearer_oauth(r):
    """
    Method required by bearer token authentication.
    """

    r.headers["Authorization"] = f"Bearer {bearer_token}"
    r.headers["User-Agent"] = "v2FullArchiveSearchPython"
    return r


def connect_to_endpoint(url, params):
    response = requests.request("GET", search_url, auth=bearer_oauth, params=params)
    print(response.status_code)
    if response.status_code != 200:
        raise Exception(response.status_code, response.text)
    return response.json()



###################################################################


#example_qry = {'query': 'from:twitterdev -is:retweet',\
#                'start_time':'2021-07-13T05:04:00.000Z', \
#                'tweet.fields':'created_at', 'expansions' : 'author_id'}


#example_qry = {'query': 'from:twitterdev (-is:retweet)',\
#                'start_time':'2021-07-19T00:00:00.000Z', \
#                'tweet.fields':'created_at', 'expansions' : 'author_id'}



#def gen_params(acct = 'twitterdev' , from_date = '2021-07-12T15:16:00.000Z',  test = 0):
#def gen_params(acct = 'twitterdev' , from_date = '2021-07-19T16:00:00.000Z',  test = 0):
#     
#    if test == 1:
#        return example_qry
#            
#    else:
#    
#        query_txt = 'from:' + acct + '(-is:retweet)'
#        
#        return {'query': query_txt , 'start_time': from_date,\
#                'tweet.fields':'created_at', 'expansions' : 'author_id'}






#####################################################################

os.chdir(r'C:\Users\luiso\Desktop\Bidness Stuff\MIDS\w266_finproj')


acc_info = pd.read_csv('accounts_data_final.csv', encoding = 'latin1')
acc_info = acc_info[acc_info.has_twitter == 1]
acc_info['twitter'] = acc_info['twitter'].str.replace("@|'", "", regex = True) 


final_df = []
request_count = 0

for index, row in acc_info.iterrows():
    username = row['twitter']
    
    start_qry = {'query': 'from: ' + username + ' (-is:retweet)',\
                    'start_time':'2019-02-01T00:00:00.000Z', \
                    'end_time':'2019-08-01T00:00:00.000Z', \
                    'max_results' : '500',\
                    'tweet.fields':'created_at', 'expansions' : 'author_id'}
        
    for i in range(4):
         
    
        print(start_qry)
        tweets = connect_to_endpoint(search_url, start_qry)
        
        time.sleep(1.1)
        request_count +=1
        
        start_qry['start_time'] = start_qry['end_time']
        
        old_qry_dt = start_qry['start_time'][:10]
        
        new_dt = datetime.strptime(old_qry_dt, '%Y-%m-%d') + relativedelta(months = 6)
        new_str = datetime.strftime(new_dt, '%Y-%m-%d')
        start_qry['end_time'] = new_str + start_qry['start_time'][10:]
        
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

        
            
            
    
    #print(tweets)
    
    

final_df_cols = list(acc_info.columns) + ['created_at', 'author_id', 'tweet_id', 'tweet_text']


final_df_pd = pd.DataFrame(final_df, columns = final_df_cols)

final_df_pd.drop_duplicates(inplace = True)

final_df_pd.to_csv('archive_tweets_final.csv')



# Optional params: start_time,end_time,since_id,until_id,max_results,next_token,
# expansions,tweet.fields,media.fields,poll.fields,place.fields,user.fields

#Params I need: 
#1.is: tweet(fixed)
#2.from: a bunch of accounts
#3. since: oct. 7 2018







#json_response = connect_to_endpoint(search_url, query_params)
#json_response = connect_to_endpoint(search_url, gen_params(test = 0))
#print(json.dumps(json_response, indent=4, sort_keys=True))
