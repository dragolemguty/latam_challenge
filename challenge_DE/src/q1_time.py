from typing import List, Tuple
from datetime import datetime
import pandas as pd
import numpy as np
from pandas import json_normalize
import json

def rank_users(df_fechas,df_users,df_retweets):
    P1_data = {'Fecha': [], 'Usuario':[]}
    P1 = pd.DataFrame(data=P1_data)
    for i in df_fechas.index:
        df_user_agrup = df_users.groupby(df_users[df_users['date']==i]['username'])['id'].count()
        df_retwet_agrup = df_retweets.groupby(df_retweets[df_retweets['date']==i]['user.username'])['user.id'].count()
        df_retwet_agrup.rename("id",inplace=True)
        df_retwet_agrup.index.name="username"
        df_merged_users = pd.merge(df_user_agrup, df_retwet_agrup, on='username', how='outer', suffixes=('_dftweets', '_dfretweets'))
        df_merged_users ['cantidad_total'] = df_merged_users ['id_dftweets'].fillna(0) + df_merged_users ['id_dfretweets'].fillna(0)
        top_1 = df_merged_users.sort_values(by='cantidad_total', ascending=False).head(1)
        P1.loc[len(P1)] = np.array([i,top_1.index[0]])    
    P1['Fecha'] = pd.to_datetime(P1['Fecha'])
    return P1

def q1_time(file_path: str) -> List[Tuple[datetime.date, str]]:
    cols = ['date','id','user','quotedTweet']
    data = []

    with open(file_path) as f:
        for line in f:
            doc = json.loads(line)
            lst = [doc['date'], doc['id'],doc['user'],doc['quotedTweet']]
            data.append(lst)

    df_P1 = pd.DataFrame(data=data, columns=cols)
    df_P1['date'] = pd.to_datetime(df_P1['date']).dt.strftime('%Y-%m-%d')

    df_P1_quotedTweet = json_normalize(df_P1['quotedTweet'])[['id','date','user.id','user.username']]
    df_P1_quotedTweet = df_P1_quotedTweet[df_P1_quotedTweet['id'].notnull()]
    df_P1_quotedTweet['date'] = pd.to_datetime(df_P1_quotedTweet['date']).dt.strftime('%Y-%m-%d')

    df_P1_users = json_normalize(df_P1['user'])[['id','username']]
    df_P1_users['date'] = df_P1['date'].values

    df_P1_agrup = df_P1.groupby(df_P1['date'])['id'].count()
    df_P1_retw_agrup = df_P1_quotedTweet.groupby(df_P1_quotedTweet['date'])['id'].count()

    df_P1_merged = pd.merge(df_P1_agrup, df_P1_retw_agrup, on='date', how='outer', suffixes=('_dftweets', '_dfretweets'))
    df_P1_merged['cantidad_total'] = df_P1_merged['id_dftweets'].fillna(0) + df_P1_merged['id_dfretweets'].fillna(0)
    df_P1_merged = df_P1_merged.sort_values(by='cantidad_total', ascending=False).head(10)

    P1 = rank_users(df_P1_merged,df_P1_users,df_P1_quotedTweet)

    output_list_P1 = [(fecha.date(), usuario) for fecha, usuario in zip(P1['Fecha'], P1['Usuario'])]
    
    return output_list_P1