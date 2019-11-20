##########################################################################################
#Sept. 17th
#Peiying Lyu
#INF 551 HW1
# 1.1 Convert the data into the JSON format and load the dataset into Firebase
############################################################################################
import json
import csv
import pandas as pd 
import requests
import sys

name = sys.argv[1]
for x in name.split():
    x = x.lower()
localPath = "D:/551 Foundations of Data Management/HW1/"
#restaurants.csv
# localPath = localPath + name
localPath = name
data = pd.read_csv(localPath)
df = pd.DataFrame(data)


groups = df.loc[0:, ['serial_number','facility_name','score']]
d = groups.set_index('serial_number').to_dict(orient='index')

#print(type(d))

json_data = json.dumps(d)
#json_data = json.load(d)
url = 'https://inf-551-lyup.firebaseio.com/restaurants.json'
response = requests.put(url,json_data)

#print(groups)
print('success: restaurants.json')

######################################################################################
#  1.2 str.split('.')
########################################################################################
#read stop words
import re

stopwords = [' ','&','@','/','\"','\'','-']
# with open('Peiying_Lyu_stopwords.txt') as ff:
#     for line in ff.readlines():
#         line = line.strip('\n')
#         stopwords.append(line)

r1 = re.compile(r'\<i\>')
r2 = '[^-a-zA-Z0-9]'

#print(stopwords)
# print(json_data.type())
#create inverted index for the facility_name of the restaurants.json
inverted_dict_list = []

data_dict = json.loads(json_data)
#print(type(data_dict))# dict
#print(data_dict.items())
for key in data_dict:
    #print(key) #serial_name
    #print(data_dict[key])
    data_dict[key].setdefault('facility_name', None)
    
    for y in data_dict[key]['facility_name'].strip('"').split():
        if y == "-":
            continue
        y=re.sub(r1,' ', y.lower())
        y=re.sub(r2,' ', y.lower())
        if y.lower not in stopwords:
            inverted_dict_list.append({y.lower().strip(',') : key})
            #print(inverted_dict_list)
            
new_dic = {}
new_inv_dict_list = []
for j in inverted_dict_list:
    for k, v in j.items():
        new_dic.setdefault(k, []).append(v)
    
new_inv_dict_list = [{k:v} for k, v in new_dic.items()]
#print(new_inv_dict_list)

for a in new_inv_dict_list:
    for b in stopwords:
        if b in a:
            del a[b]

while {} in new_inv_dict_list:
    new_inv_dict_list.remove({})

#print(new_inv_dict_list)

new_inv_dict={}
for dic in new_inv_dict_list:
    dic_key_list = list(dic.keys())
    dic_valus_list = list(dic.values())
    new_inv_dict.setdefault(dic_key_list[0], dic_valus_list[0]) #
#print(new_inv_dict)

with open('index.json', 'w') as f2:
    json.dump(new_inv_dict, f2)

with open('index.json', 'r') as f3:
    index_data = json.load(f3)

response = requests.put('https://inf-551-lyup.firebaseio.com/index.json', json=new_inv_dict)
#str1.split('.')
print('success: index.json')