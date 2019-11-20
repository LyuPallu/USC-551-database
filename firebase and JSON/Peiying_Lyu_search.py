#######################################
#2. search.py
#######################################
import json
import csv
import pandas as pd 
import requests
import sys

name = sys.argv[1]
for x in name.split():
    x = x.lower()
# name = 'taco zwei'
#name = 'zwei'

for x in name.split():
    x = x.lower()
    url1 = 'https://inf-551-lyup.firebaseio.com/index/'
    url1 = url1 + x+ '.json'
    response1 = requests.get(url1)

   # print( response1.json())
    #print(type(response1.json()))
    
    # text = []
    text = response1.json()
    #print(type(text))
    
    for i in text:
        url2 = 'https://inf-551-lyup.firebaseio.com/restaurants/'
        url2 = url2 + i +'.json'
        # print(type(i))
        response2 = requests.get(url2)
        print("\"",i,"\"",":")
        print(response2.json())
        print(",")
        