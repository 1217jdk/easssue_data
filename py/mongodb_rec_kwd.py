# -*- coding: cp949 -*-

import sys
import mysql.connector
import datetime
from wordcloud import WordCloud
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from sqlalchemy import create_engine
import pymysql
import urllib
import time
import pickle

from surprise.model_selection import train_test_split
from surprise import Dataset
from surprise import Reader
from surprise import NMF
from surprise import SVD
from surprise.model_selection import cross_validate

from pymongo import MongoClient

with open('/root/Data/data/mongodb_user_info.pickle', 'rb') as f:
    user_info = pickle.load(f)

client = MongoClient(host='www.easssue.com', port=27017, username=user_info['username'], password=user_info['password'])
db = client.get_database('easssue_data')
coll_al = db.get_collection('articleLog')
coll_a = db.get_collection('article')

with open(f'/root/Data/data/kwd/20221117_01_kwd_name_id.pickle', 'rb') as f:
    kwds_name_id = pickle.load(f)


def get_user_word(user_id):
    today = datetime.datetime.today() + datetime.timedelta(hours=9)
    week_ago = today - datetime.timedelta(days=7)

    result = []
    articles = [item["articleId"] for item in coll_al.find({"userId": user_id, "clickTime": {"$gte": week_ago, "$lte": today}})]
    for article in articles:
        for item in coll_a.find({"articleId": article}):
            for kwd in item['kwds']:
                result.append((kwds_name_id[kwd['kwd']], kwd['kwdCount']))

    return result


mysql_df = pd.read_csv(f'/root/Data/data/mysql.csv')
password = mysql_df.loc[0, 'password']

mydb = mysql.connector.connect(
    host="www.easssue.com",
    user="root",
    password=password,
    database="easssue_data"
)

mycursor = mydb.cursor()

mycursor.execute('select user_id from users')
users = mycursor.fetchall()
# print(users)

mycursor.execute('select kwd_id, kwd_name from kwd')
kwds = mycursor.fetchall()
kwds_df = pd.DataFrame(kwds, columns=['kwd_id', 'kwd_name'])
# print(kwds)


user_vocab = pd.DataFrame()
for user_id in users:
    user_vocab_lst = dict(get_user_word(user_id[0]))
    user_vocab_lst = dict(sorted(user_vocab_lst.items(), key=lambda item: item[1]))
    user_vocab_df = pd.DataFrame(user_vocab_lst.items(), columns=['kwd_id', 'rating'])
    user_vocab_df['user_id'] = user_id[0]
    user_vocab_df.sort_values(by=['rating'], ascending=False, inplace=True, ignore_index=True)
    user_vocab_df = user_vocab_df[['user_id', 'kwd_id', 'rating']]
    user_vocab = pd.concat([user_vocab, user_vocab_df], ignore_index=True)


def sigmoid(x):
    return 1 / (1 + np.exp(-0.5 * x))


user_vocab['rating'] = user_vocab['rating'].apply(sigmoid)
# print(user_vocab)


# surprise???? ???????? ???? dataframe -> dataset ???? ????
# reader : ???? ???? ????
reader = Reader(rating_scale=(0.5, 1))
data = Dataset.load_from_df(user_vocab, reader)

## from surprise.model_selection import GridSearchCV

## param_grid = {"n_epochs": [20, 30, 50, 70], "lr_all": [0.002, 0.003, 0.005, 0.007, 0.01], "reg_all": [0.02, 0.03, 0.05, 0.07]}
## gs = GridSearchCV(SVD, param_grid, measures=["rmse", "mae"], cv=3)

## gs.fit(data)

### best RMSE score
## print(gs.best_score["rmse"])

### combination of parameters that gave the best RMSE score
## print(gs.best_params["rmse"])

## sys.exit()


# trainset, testset = train_test_split(data, test_size=.2, random_state=1115)
trainset = data.build_full_trainset()
algo = SVD(n_epochs=30, lr_all=0.007, reg_all=0.07)
algo.fit(trainset)
# prediction = algo.test(testset)
# print(prediction)


from surprise import accuracy


def get_unseen_surprise(df, kwds_df, userId):
    # ???? ?????? ?? kwd id???? ???????? ????
    seen_kwds = df[df['user_id'] == userId]['kwd_id'].tolist()
    # print(f'{userId} ?????? ?? ??????: {[kwds_df[kwds_df["kwd_id"]==kwd_id]["kwd_name"].values[0] for kwd_id in seen_kwds]}')

    # ???? ?????? ??????
    kwds_lst = kwds_df['kwd_id'].tolist()
    # ???? ?????????? kwd id?? ?? ???? ?????? ?? kwd id?? ?????? ?????? ????
    unseen_kwds = [kwd for kwd in kwds_lst if kwd not in seen_kwds]

    # print(f'???? {userId}?? ?????? ?? ?????? ??: {len(seen_kwds)}?n?????? ?????? ????: {len(unseen_kwds)}?n???? ????????: {len(kwds_lst)}')

    return unseen_kwds


def recomm_kwd_by_surprise(algo, userId, unseen_kwds, top_n=10):
    # ???????? ?????? predict()?? ?????? ???? userId?? ?????? ???? ???????? ???? ???? ????
    predictions = [algo.predict(userId, kwd_id) for kwd_id in unseen_kwds]

    # print(predictions)

    # predictions?? Prediction()???? ?????? ?????? ???????? ?????? ????????(est??)?? ???????? ??????????
    # est???? ???????? ???????? ????. ?????? ?????? ???????? ???????? sort()?????? key???? ????????!
    def sortkey_est(pred):
        return pred.est

    # sortkey_est?????? ???????? ???????? sort?????? key?????? ????????
    # ?????? sort?? ?????????? inplace=True?? ?????? ???????? ??????. reverse=True?? ????????
    predictions.sort(key=sortkey_est, reverse=True)
    # ???? n???? ?????????? ????
    top_predictions = predictions[:top_n]
    # print(top_predictions)
    ## top_predictions ex : (uid : user_id, iid: item_id, est : rating)
    # top_predictions???? kwd id, rating, kwd_name ?? ????????
    top_kwd_ids = [int(pred.iid) for pred in top_predictions]
    top_kwd_ratings = [pred.est for pred in top_predictions]
    top_kwd_names = [kwds_df[kwds_df['kwd_id'] == kwd_id]['kwd_name'].values[0] for kwd_id in top_kwd_ids]
    # ?? 3?????? ?????? ????
    # zip?????? ???????? ?? ????????(?????? ??????)?? ?????? ?????????? ?????? mapping
    # zip?????? ?????? ???????? ???????? ?????? ?????????? mapping?? ????!
    top_kwd_preds = [(userId, ids, rating, name) for ids, rating, name in
                     zip(top_kwd_ids, top_kwd_ratings, top_kwd_names)]

    return_df = pd.DataFrame(top_kwd_preds, columns=['user_id', 'kwd_id', 'score', 'kwd_name'])
    return return_df


### ?????? ?????? ?????? ?????? ???? ?????? ???? ???????? ??????????
# unseen_lst = get_unseen_surprise(user_vocab, kwds_df, 1)
# top_kwds_preds = recomm_kwd_by_surprise(algo, 1, unseen_lst, 10)

# for top_kwd in top_kwds_preds:
# print('* ???? ?????? ????: ', top_kwd[2])
# print('* ???? ?????? ????????: ', top_kwd[1])
# print()

result = pd.DataFrame()
for user in users:
    unseen_lst = get_unseen_surprise(user_vocab, kwds_df, user[0])
    top_kwds_preds = recomm_kwd_by_surprise(algo, user[0], unseen_lst, 10)
    result = pd.concat([result, top_kwds_preds])

today = datetime.datetime.now() + datetime.timedelta(hours=9)
today = today.date().strftime('%Y-%m-%d')
result['reg_date'] = today
result.drop(columns=['kwd_name'], inplace=True)

print(result.head())


### DB?? ????
## DB ????????


host = "www.easssue.com:3306"
user = "root"
password = password
database = "easssue_data"

db_connection_str = f'mysql+pymysql://{user}:{password}@{host}/{database}'
db_connection = create_engine(db_connection_str)
conn = db_connection.connect()

result.to_sql(name='rec_kwd', con=db_connection, if_exists='append', index=False)
print('???????????? ?????????? db?? ??????????????. ')
