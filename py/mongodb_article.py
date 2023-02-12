from konlpy.tag import Mecab
from collections import Counter
import pandas as pd
import pickle
import copy
import json
import datetime

# 1. 키워드 정제하는 함수 정의
from tqdm import tqdm


def preprocessing1(sorted_kwds_lst):
    delete_set = set()
    for i in tqdm(range(len(sorted_kwds_lst) - 1)):
        kwd1, cnt1 = sorted_kwds_lst[i]
        for j in range(i + 1, len(sorted_kwds_lst)):
            kwd2, cnt2 = sorted_kwds_lst[j]

            # kwd1이 삭제가 안된다면 break
            if (cnt1 * 0.65 >= cnt2):
                break

            if kwd1 in kwd2:  # and (cnt1*0.65 <= cnt2):
                delete_set.add(kwd1)
                # kwd1은 삭제 확정이므로 다음 루프로 이동
                break
    # print(f'전체 {len(sorted_kwds_lst)} 중, {len(delete_set)} 개를 삭제헤야 됨!')

    return delete_set

# 2. 메타정보 가져오기
meta_df = pd.read_csv('/root/Data/data/meta.csv')
print('-' * 50)
print('meta data is ')
print(meta_df)
print('-' * 50)

max_article_id, article_filedate, article_try, kwd_filedate, kwd_try = meta_df.loc[
    0, ['max_article_id', 'article_filedate', 'article_try', 'kwd_filedate', 'kwd_try']]
article_try = str(article_try).zfill(2)  # 01과 같은 형식으로 만들어 주기
kwd_try = str(kwd_try).zfill(2)  # 01과 같은 형식으로 만들어 주기

# 3. Data 가져오기
article_df = pd.read_csv(f'/root/Data/data/article/{article_filedate}_{article_try}_article_db.csv')
article_df = article_df.drop_duplicates(subset=['link'])  # 중복 삭제

new_kwds = pd.read_csv(f'/root/Data/data/kwd/{kwd_filedate}_{kwd_try}_kwd.csv')
with open(f'/root/Data/data/kwd/{kwd_filedate}_{kwd_try}_kwd_name_id.pickle', 'rb') as f:
    kwds_name_id = pickle.load(f)

# 4. 키워드 개수 세기
mecab = Mecab()

article_id_lst = []
kwd_lst = []
kwd_count_lst = []
article_kwd_dict = dict()
for i in range(len(article_df)):
    row = article_df.iloc[i]
    article_id = row['article_id']
    desc = row['description']

    nouns = mecab.nouns(desc)
    nouns_counter = dict(Counter(nouns))

    ## [1] 키워드 저장하기
    kwds = []

    ### (1) 1글자 키워드
    for kwd in new_kwds[new_kwds['kwd_name'].map(len) == 1]['kwd_name']:
        if kwd in nouns:
            kwds.append((kwd, nouns_counter[kwd]))

    ### (2) 2글자 이상 키워드
    for kwd in new_kwds[new_kwds['kwd_name'].map(len) > 1]['kwd_name']:
        if kwd in desc:
            # kwds.append((str(article_id), str(kwds_name_id[kwd]), desc.count(kwd)) )
            kwds.append((kwd, desc.count(kwd)))
    '''
    article_id = i 의 결과에 해당 됨.
    kwds = [
    (경제, 31), (미국, 2),  금리, 9), ( keyword_name, keyword_count),  ... 꼴
    ]
    '''

    ## [2] 키워드 정제하기
    ## 많이 나온 것 부터 정렬하고, 같은 횟수 나왔으면 글자길이 적은 것부터 정렬- > [ (i, '로나',1923), (i, '코로', 1921), (i, '코로나', 1871), ... ,(i, '머스캣',1), (i, '샤인머스캣',1) ,...] 꼴로 정렬되게 하기!

    ### (1) 글자가 짧은 것이 앞에 오도록 정렬
    sorted_kwds_lst = sorted(kwds, key=lambda item: len(item[0]), reverse=False)

    ### (2) count가 많이 된 것이 앞에 오도록 정렬
    sorted_kwds_lst = sorted(sorted_kwds_lst, key=lambda item: item[1], reverse=True)
    # print(sorted_kwds_lst)

    ### (3) 삭제할 것 찾기
    delete_set = preprocessing1(sorted_kwds_lst)
    # print(f'삭제할 목록 : {delete_set}')

    ### (4) 삭제할 것 삭제하기

    if delete_set:
        deleted_kwds_dict = {kwdName_count[0]: kwdName_count[1] for kwdName_count in
                             sorted_kwds_lst}  # { kwd_name : kwd_count } 꼴로 만들기
        for delete_kwd in delete_set:  # kwd_id가 들어있는 상태
            del deleted_kwds_dict[delete_kwd]

        ### (5) 삭제 후에 다시 sorted_kwds_lst 만들기
        sorted_kwds_lst = sorted(deleted_kwds_dict.items(), key=lambda item: item[1], reverse=True)  # kwd 많이 나온 순으로 정렬
        # print('삭제한 이후의 결과')
        # print(sorted_kwds_lst)

    ## [3] sorted_kwds_lst 정렬하기
    ### (1) (count 높은 순, 글자 긴 순) 으로 정렬하기
    sorted_kwds_lst = sorted(sorted_kwds_lst, key=lambda item: len(item[0]), reverse=True)
    # print('글자 길이순으로 정렬')
    # print(sorted_kwds_lst)
    sorted_kwds_lst = sorted(sorted_kwds_lst, key=lambda item: item[1], reverse=True)
    ''' ( count 순, 글자길이 긴 순) 정렬 
    sorted_kwds_lst =
    [('데이터', 10), ('도서관', 6), ('빅데이터', 4), ('인천', 4), ('재난', 4), ('동아', 4), ('미래', 3), ('데이터 분석', 2), ('서비스', 2), ('날씨', 2), ('태풍', 2), ('황사', 2), ('한파    ', 2), ('폭염', 2), ('학생', 2), ('조선', 2), ('제시', 2), ('뉴스', 2), ('재해', 2), ('스토리텔링', 1), ('대학교', 1), ('협업', 1), ('배포', 1), ('사업', 1), ('자유', 1), ('불어', 1    ), ('태현', 1), ('지우', 1), ('추천', 1)]

    '''
    # print()
    # print('count 순으로 정렬')
    # print(sorted_kwds_lst)

    ## [4] DB에 넣을 형식으로 바꾸기

    if sorted_kwds_lst:  # 데이터가 존재한다면
        cur_kwd_lst = []
        for kwd_name, kwd_count in sorted_kwds_lst:
            cur_kwd_lst.append({'kwd': kwd_name, 'kwdCount': kwd_count})

        ### (3) DF만들 것에 넣어주기
        article_id_lst.append(article_id)
        kwd_lst.append(cur_kwd_lst)

    # print()
    # print(article_id_lst)
    # print(kwd_lst)
    # print(kwd_count_lst)
    # break


article_kwd_df = pd.DataFrame({'article_id': article_id_lst, 'kwds': kwd_lst})
# print('-' * 50, '결과 df', '-' * 50)
# print(article_kwd_df)

article_db_df = pd.merge(left = article_df , right = article_kwd_df, how = "left", on = "article_id")

article_db_df['kwds'].fillna({i: [] for i in article_db_df.index}, inplace=True)
# ['article_id', 'category_id', 'title', 'link', 'pub_date', 'hit',
#        'summary', 'img', 'description', 'from_kwd_name', 'from_kwd_id', 'kwds']
article_db_df.rename(columns = {'article_id': 'articleId', 'category_id': 'category', 'from_kwd_name': 'fromKwd', 'pub_date': 'pubDate'}, inplace = True)

# {'IT/과학': 1, '경제':2, '문화/생활': 3, '미용/건강': 4, '사회': 5, '스포츠': 6, '연예': 7, '정치': 8}
category_name_id = {1: 'IT/과학', 2: '경제', 3: '문화/생활', 4: '미용/건강', 5: '사회', 6: '스포츠', 7: '연예', 8: '정치'}
article_db_df['category'] = article_db_df['category'].apply(lambda x: category_name_id[x])

article_db_df['summary'] = article_db_df['summary'].apply(lambda x: x.split('\n'))

# article_db_df['pubDate'] = article_db_df['pubDate'].apply(lambda x: {'$date':datetime.strptime(x, '%Y-%m-%d %H:%M:%S').isoformat(timespec='milliseconds')+'Z'})

article_db_df = article_db_df[['articleId', 'category', 'fromKwd', 'title', 'link', 'pubDate', 'hit', 'summary', 'img', 'kwds']]

article_db_json = json.loads(article_db_df.to_json(force_ascii=False, orient='records'))

for row in article_db_json:
    row['pubDate'] = datetime.datetime.strptime(row['pubDate'], '%Y-%m-%d %H:%M:%S') - datetime.timedelta(hours=9)

# 4. DB에 넣기

## [1] DB에 연결
from pymongo import MongoClient

with open('/root/Data/data/mongodb_user_info.pickle', 'rb') as f:
    user_info = pickle.load(f)

client = MongoClient(host='www.easssue.com', port=27017, username=user_info['username'], password=user_info['password'])
db = client.get_database('easssue_data')
coll = db.get_collection('article')

## [2] DB에 넣기

coll.insert_many(article_db_json)