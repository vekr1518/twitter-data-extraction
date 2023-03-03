import requests
import json
import pandas as pd
import os

#Fetch the API Secret Key and host details from env variables
api_host = os.environ.get('RapidAPI-Host')
api_key = os.environ.get('RapidAPI-Key')


url = "https://twitter-data1.p.rapidapi.com/UserByScreenName/"
querystring = {"username":"ImRo45"}
headers = {
	"X-RapidAPI-Key": api_key,
	"X-RapidAPI-Host": api_host
}

r=requests.get(url,headers=headers,params=querystring)
twitterdata=r.json()

array_list=[]
for k,v in twitterdata.items():
	for i in v:
		for j in v[i]:
			id = v[i][j]['rest_id']
			screen_name=v[i][j]['legacy']['screen_name']
			description=v[i][j]['legacy']['description']
			created_at=v[i][j]['legacy']['created_at']
			name=v[i][j]['legacy']['name']
			followers=v[i][j]['legacy']['followers_count']
			following=v[i][j]['legacy']['friends_count']
			location=v[i][j]['legacy']['location']
			account_verified=v[i][j]['legacy']['verified']
			array_element = {'Id':id,'Name':name,'Description':description,'Screen_name':screen_name,'Created_at':created_at,'Followers':followers,
			 'Following':following,'Location':location,'Account_verified':account_verified}
			array_list.append(array_element)
			#print(array_list)

twitter_df = pd.DataFrame.from_dict(array_list)
print(twitter_df.to_string())





























































