#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Apr 13 10:32:48 2018

@author: adrianbenitez
"""
import requests
from requests_oauthlib import OAuth1
import time
import pandas as pd
from sqlalchemy import create_engine, select
import pymssql
from itertools import chain
import numpy as np


users = pd.read_excel('zodiask_users.xlsx')

user_list = list(users['USER_ID'].astype(str))

#List of dictionaries with the access tokens for the API
accesos = ({'apikey':'',

            'apisecret':'',

            'accesstoken':'',

            'accesssecre':''},

           {'apikey':'',

            'apisecret':'',

            'accesstoken':'',

            'accesssecre':''},

           {'apikey':'',

            'apisecret':'',

            'accesstoken':'',

            'accesssecre':''},

            {'apikey':'',

            'apisecret':'',

            'accesstoken':'',

            'accesssecre':''},

            {'apikey':'',

            'apisecret':'',

            'accesstoken':'',

            'accesssecre':''})

#Parameters for authentification
count = 0
auth = OAuth1(accesos[count]['apikey'], accesos[count]['apisecret'],
              accesos[count]['accesstoken'], accesos[count]['accesssecre'])

urlfollowing =  'https://api.twitter.com/1.1/friends/ids.json'
user_info = 'https://api.twitter.com/1.1/users/lookup.json'

#Creation of an empty dataframe to stopre the data later on
usuario_friend = pd.DataFrame()

#inicio will be the starting timer, later will be rested whenever the token
#changes. Call is a checking parameter and user will be 0 as it is the first
#index of the list
inicio = time.time()
call = 0
user = 3194
while user < len(user_list):
    #Set to zero and empty the initial variables for each loop
    x = 0
    cursor = []
    #last cursor -1 returns the first page of the iteration
    last_cursor = -1
    #parameters for the looping every user using the last cursor if there are
    #more than 5000 friends
    new_param = {'user_id': user_list[user],
                     'cursor': last_cursor}
    friends = requests.get(urlfollowing, auth=auth,
                               params = new_param)
    #Here we will store the status of the call
    friends_status = friends.status_code
    #Check out the status before generating the json
    if friends_status == 200:
        #If the status is successful we generate the json and we will start
        #exctracting ids of the followings.
        friends = friends.json()
        call += 1
        #Mapping the ids as strings
        friends_ids = list(map(str,friends['ids']))
        #we save the next cursor in case there is one
        last_cursor = friends['next_cursor_str']
        #if the cursor is different than 0, that means that there is more pages
        while friends['next_cursor'] != 0:
            #we change the cursor for the next page
            new_param = {'user_id': user_list[user],
                             'cursor': last_cursor}

            add_friends = requests.get(urlfollowing, auth=auth,
                                      params = new_param)
            add_status = add_friends.status_code
            #If the call is successful we will procede to add the rest of the
            #friends to the list
            if add_status == 200:
                friends = add_friends.json()
                call += 1
                cursor.append(friends['next_cursor_str'])
                add_friends = list(map(str, friends['ids']))
                friends_ids.append(add_friends)
                print(call)
                continue
            #If we have exceeded the limitation of calls the token will be
            #changed
            elif add_status == 420 or add_status == 429:
                #if count is bigger than the number of access it will
                #return to 0
                count +=1
                if count >= len(accesos):
                    count = 0

                auth = OAuth1(accesos[count]['apikey'],
                              accesos[count]['apisecret'],
                              accesos[count]['accesstoken'],
                              accesos[count]['accesssecre'])

                #Since the last call has given us an error
                #next_cursor_str will not be available, so last_cursor
                #will be updated  with the last value saved in the
                #cursor list. Cursor list maybe empty
                if len(cursor) != 0:
                    last_cursor = cursor[-1]
                fin = time.time()
                tiempo = fin - inicio
                #We have calculated the time since the last time we changed
                #the token. So every token have to wait 3 minutes to not exceed
                #the limits of calls
                if 180 - tiempo > 0:
                    print('Espera de %d segundo en %d llamadas' %
                          (180-tiempo, call))
                    time.sleep(181 - tiempo)
                #Reseting successful calls and time
                inicio = time.time()
                call = 0
                continue
            #Unnesting the nested list in case there has been more than 5000
            friends_ids = list(chain(*friends_ids))
        #If the user is following less than a hundred
        if len(friends_ids) < 100:
            #n will be the length of the friends_ids
            n = len(friends_ids)
        else:
            #if not, it will be 100
            n = 100
        t = 0
        #While n is lesser or the same as the length of the list and more than
        #t, the next loop will be runing.
        while n <= len(friends_ids) and t < n:
            #Now qe are getting the data of the followings in chunks of t to n
            new_data = {'user_id': friends_ids[t:n],
                            'include_entities': 'false',
                            'tweet_mode': 'false'}
            users_data = requests.post(user_info, auth = auth,
                               data = new_data)
            user_status = users_data.status_code

            if user_status == 200:
                #If the status is correct they will be uploaded into the
                #dataframe
                users_data =users_data.json()
                for i in range(len(users_data)):

                        if users_data[i]['verified'] == True:
                            #Saved follower and followings
                            temp = pd.DataFrame({'USER_ID' : user_list[user],
                                    'FOLLOWING': users_data[i]['screen_name']},
                            index = [0])
                            usuario_friend = pd.concat([usuario_friend, temp])
                        else:
                            continue
            else:
                #If there are any errors we will print it here
                print('Error con el usuario %s con status code %d' %
                      (user_list[user], user_status))
                pass
            #Adding the next hundred users and getting the difference if they
            #are not multiple of a hundred
            n += 100
            t += 100
            rest = len(friends_ids) - n
            #In the case they are not n is going to take only the rest of users
            if rest < 0:
                n = n + rest
        #Print check and next user, reseting cursor
        print('Añadido usuario %d de la lista' % user)
        user += 1
        cursor = []
    #this status code checking is for the first get request of the initial
    #while loop
    elif friends_status == 420 or friends_status == 429:
        count +=1
        #if count is bigger than the number of access it will
        #return to 0
        if count >= len(accesos):
            count = 0
        #change of authentification and saving the dataframe in a pickle
        auth = OAuth1(accesos[count]['apikey'], accesos[count]['apisecret'],
              accesos[count]['accesstoken'], accesos[count]['accesssecre'])
        usuario_friend.to_pickle('us_friend')
        #We have calculated the time since the last time we changed
        #the token. So every token have to wait 3 minutes to not exceed
        #the limits of calls
        fin = time.time()
        tiempo = fin - inicio
        if 180 - tiempo > 0:
            print('Espera de %d segundos en %d llamadas' % (180-tiempo, call))
            time.sleep(181 - tiempo)

        inicio = time.time()
        call = 0
        continue
   #If the error happens because it can extract the info from the user...
    elif friends_status >= 401 and friends_status <= 404:
        #Pass to the next user
        user += 1
        print('Usuario privado')
        continue
#Drop any possible duplicate
usuario_friend.drop_duplicates(inplace = True)
usuario_friend.to_pickle('us_friend')

