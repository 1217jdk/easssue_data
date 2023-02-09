from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
import time
from datetime import datetime, timedelta

# browser = webdriver.Chrome('/usr/local/bin/chromedriver')
browser = webdriver.Chrome('/opt/homebrew/bin/chromedriver')
# browser = webdriver.Chrome('./chromedriver.exe')

browser.implicitly_wait(3)

URL = 'https://signal.bz/news/'
browser.get(url=URL)
# time.sleep(3)

naver_results = browser.find_elements(By.CSS_SELECTOR,'#app > div > main > div > section > div > section > section:nth-child(2) > div > div > div > div > a > span.rank-text')

naver_list = []

for naver_result in naver_results:
    naver_list.append(naver_result.text)

print(naver_list)

## DB에 넣기
rank = range(1, 11)
reg_date = [datetime.now() + timedelta(hours=9) for _ in range(10)]

import pandas as pd
import mysql.connector
from sqlalchemy import create_engine

trend_df = pd.DataFrame({'rank': rank, 'title': naver_list, 'reg_date': reg_date})
# print(trend_df.head())

mysql_df = pd.read_csv(f'/root/Data/data/mysql.csv')
password = mysql_df.loc[0, 'password']

host = "www.easssue.com:3306"
user = "root"
password = password  # 특수문자 때문에, parse 해줘야 함
database = "easssue_data"

db_connection_str = f'mysql+pymysql://{user}:{password}@{host}/{database}'
db_connection = create_engine(db_connection_str)
conn = db_connection.connect()

trend_df.to_sql(name='trend', con=db_connection, if_exists='append', index=False)