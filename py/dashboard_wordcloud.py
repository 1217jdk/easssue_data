# -*- coding: cp949 -*-

import sys,os
import mysql.connector
import datetime
from wordcloud import WordCloud
import numpy as np
import matplotlib.pyplot as plt
import json
import pickle
from pymongo import MongoClient


def color_func(word, font_size, position,orientation,random_state=None, **kwargs):
    return("hsl({:d},{:d}%, {:d}%)".format(np.random.randint(180,220),np.random.randint(70,90),np.random.randint(50,70)))


with open('./mongodb_user_info.pickle', 'rb') as f:
    user_info = pickle.load(f)

client = MongoClient(host='www.easssue.com', port=27017, username=user_info['username'], password=user_info['password'])
db = client.get_database('easssue_data')
coll_al = db.get_collection('articleLog')
coll_a = db.get_collection('article')

with open(f'./20221117_01_kwd_name_id.pickle', 'rb') as f:
    kwds_name_id = pickle.load(f)


def get_user_word(user_id, cursor):
    today = datetime.datetime.today()
    week_ago = today - datetime.timedelta(days=7)

    result = []
    articles = [item["articleId"] for item in
                coll_al.find({"userId": int(user_id), "clickTime": {"$gte": week_ago,}})]
    for article in articles:
        for item in coll_a.find({"articleId": article}):
            for kwd in item['kwds']:
                result.append((kwd['kwd'], kwd['kwdCount']))

    return result

def main(argv):
    if not os.path.exists('./img/dash'):
        os.mkdir('./img/dash')

    user_id = argv[1]

    # --------------------------
    base_dir=os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    secret_file=os.path.join(base_dir,'resources/secrets.json')

    with open(secret_file) as f:
        secrets=json.loads(f.read())

    def get_secret(key, secrets=secrets):
        return secrets[key]

    mysql_pwd=get_secret("mysql_pwd")
    # --------------------------

    mydb = mysql.connector.connect(
        host="www.easssue.com",
        user="root",
        password=mysql_pwd,
        database="easssue_data"
    )

    mycursor = mydb.cursor()

    user_vocab_lst = get_user_word(user_id, mycursor)
    user_vocab_lst.sort(key=lambda x: x[1], reverse=True)
    user_vocab_lst = dict(user_vocab_lst[:25])
    # print(user_vocab_lst)
    ### 워드클라우드 제작
    wc = WordCloud(background_color='white', font_path='./SB ¾?±×?? B.ttf', color_func=color_func, width=945, height=241)
    wc.generate_from_frequencies(user_vocab_lst)

    ## 파일로 저장
    now = datetime.datetime.now().strftime('%Y-%m-%d_%H:%M:%S:%f')
    wc.to_file(f'./img/dash/{now}.png')

    ## show
    # figure = plt.figure(figsize=(12, 12))
    # ax = figure.add_subplot(1, 1, 1)
    # ax.axis('off')
    # ax.imshow(wc)
    # plt.show()
    print(now)

    return now

if __name__ == '__main__':
    main(sys.argv)