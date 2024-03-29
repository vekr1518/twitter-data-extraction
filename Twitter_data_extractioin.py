from pyspark.sql import SparkSession
import requests
import json
import pandas as pd
import os

#Initiate Spark Session

spark = SparkSession.builder.appName("MyTwitterExtract")\
    .config("spark.jars", "/Users/vk/spark/postgresql-42.5.1.jar")\
    .config("spark.driver.extraClassPath", "/Users/vk/spark/postgresql-42.5.1.jar")\
    .getOrCreate()
sc=spark.sparkContext


#Connection Details
PSQL_SERVERNAME = "localhost"
PSQL_PORTNUMBER = 5432
PSQL_DBNAME = "<dbname>"
PSQL_USERNAME = "<user-name>"
PSQL_PASSWORD = "<password>"

URL = f"jdbc:postgresql://{PSQL_SERVERNAME}:{PSQL_PORTNUMBER}/{PSQL_DBNAME}"

#Fetch the API Secret Key and host details from env variables
api_host = os.environ.get('RapidAPI-Host')
api_key = os.environ.get('RapidAPI-Key')

url = "https://twitter-data1.p.rapidapi.com/UserByScreenName/"
headers = {
	"X-RapidAPI-Key": api_key,
	"X-RapidAPI-Host": api_host
}

user = ["imVkohli","ImRo45","elonmusk","sundarpichai","cristiano"]

array_list=[]
for row in user:
    querystring = {"username":row}
    r=requests.get(url,headers=headers,params=querystring)
    data=r.json()
    for k,v in data.items():
        for i in v:
            for j in v[i]:
                id=v[i][j]['rest_id']
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

df= spark.createDataFrame(array_list)

#Load data to postgres
df.write.format("jdbc")\
   .option("url",URL)\
   .option("dbtable","Twitter_User_Detail")\
   .option("user",PSQL_USERNAME)\
   .option("password",PSQL_PASSWORD)\
   .mode("append")\
   .save()




