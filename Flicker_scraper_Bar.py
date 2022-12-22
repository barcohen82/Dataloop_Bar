from flickrapi import FlickrAPI
import pandas as pd 
from datetime import datetime
import mysql.connector
from sqlalchemy import create_engine


# Flickr API access
key = ''
secret = ''

# Scrape params
Scrape_keyword = ''
Scrape_size = 20 #change if needed

# MySQL params
hostname=''
dbname = ''
uname = ''
pwd = ''
SQL_table_name = 'images'

# Search params
Search_table_name = 'images'
minScrapeTime = '2022-12-22 10:20:00' #change if needed
maxScrapeTime = '2022-12-22 10:39:00' #change if needed  
keyword1 = ''
size1 = 5 #change if needed


def Scrape(keyword,size):
    # connect to FlickrAPI
    flickr = FlickrAPI(key, secret)

    now = datetime.now()

    # Get the required information from the photos 
    photos = flickr.walk(text=keyword,
                            tag_mode='all',
                            tags=keyword,
                            extras='url_o',
                            per_page=10,
                            sort='relevance')
    count=0
    imageUrl = []
    scrapeTime = []
    keyword2 = []

    for photo in photos:
        if count < size:
            url=photo.get('url_o')
            if url != None:
                count=count+1
                imageUrl.append(url)
                time = now.strftime("%Y-%m-%d %H:%M:%S")
                scrapeTime.append(time)
                keyword2.append(keyword)
        else:
            break
    table = pd.DataFrame(list(zip(imageUrl, scrapeTime,keyword2)),
               columns =['imageUrl', 'scrapeTime','keyword'])
    return table



def MySQL(hostname,dbname,uname,pwd,table_name,df):

    # connect to MySQL and create database if not exists
    mydb = mysql.connector.connect(
                                    host=hostname,
                                    port='330', # change the port if needed
                                    user=uname,
                                    password=pwd
                                    )
   
    mycursor = mydb.cursor()
    mycursor.execute(f"CREATE DATABASE IF NOT EXISTS {dbname}")

    # connect to MySQL database and create table if not exists
    conn = mysql.connector.connect(host=hostname,
                                        port='330', # change the port if needed
                                         database=dbname,
                                         user=uname,
                                         password=pwd)
    
    cursor = conn.cursor()

    cursor.execute(f"CREATE TABLE IF NOT EXISTS {table_name} (imageUrl VARCHAR(255), scrapeTime TIMESTAMP,keyword VARCHAR(255))")

    engine = create_engine("mysql://{user}:{pw}@{host}:330/{db}" # change the port if needed
				.format(host=hostname, db=dbname, user=uname, pw=pwd))

    # upload data from dataframe to MySQL
    df.to_sql(f'{table_name}', engine,if_exists='append', index=False)

    return engine



def Search(minScrapeTime,maxScrapeTime,keyword1,size1,table_name,engine):
    # the query that will be sent to MySQL
    query = f"SELECT * FROM {table_name} WHERE keyword='{keyword1}' AND scrapeTime BETWEEN '{minScrapeTime}' AND '{maxScrapeTime}' LIMIT {size1}" 
    
    # convert SQL Query Results to a Pandas Dataframe
    df = pd.read_sql(query, con=engine)
    return df




table = Scrape(Scrape_keyword,Scrape_size)

engine = MySQL(hostname,dbname,uname,pwd,SQL_table_name,table)

df = Search(minScrapeTime,maxScrapeTime,keyword1,size1,Search_table_name,engine)

# print(df)

    