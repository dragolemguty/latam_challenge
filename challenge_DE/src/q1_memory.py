from typing import List, Tuple
from datetime import datetime
import pandas as pd
import numpy as np
from pandas import json_normalize
import gc
import json

#Funcion utilizada para encontrar los usuarios con mayor cantidad de tweets dado un dataframe con fechas
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

def q1_memory(file_path: str) -> List[Tuple[datetime.date, str]]:

    #Lectura especifica de las columnas de interes
    cols = ['date','id','user','quotedTweet']
    data = []
    with open(file_path) as f:
        for line in f:
            doc = json.loads(line)
            lst = [doc['date'], doc['id'],doc['user'],doc['quotedTweet']]
            data.append(lst)
    
    #Generacion de DataFrame
    df_P1 = pd.DataFrame(data=data, columns=cols)
    df_P1['date'] = pd.to_datetime(df_P1['date']).dt.strftime('%Y-%m-%d')
    df_P1['id'] = df_P1['id'].astype('int8') #MEM
    cols.clear() #MEM
    data.clear() #MEM

    #Se expande y se obtienen columnas de interés de columna quotedTweet
    df_P1_quotedTweet = json_normalize(df_P1['quotedTweet'])[['id','date','user.id','user.username']]
    df_P1_quotedTweet = df_P1_quotedTweet[df_P1_quotedTweet['id'].notnull()]
    df_P1_quotedTweet['id'] = df_P1_quotedTweet['id'].astype('int8')#MEM
    df_P1_quotedTweet['date'] = pd.to_datetime(df_P1_quotedTweet['date']).dt.strftime('%Y-%m-%d')

    #Se expande y se obtienen columnas de interés de columna user
    df_P1_users = json_normalize(df_P1['user'])[['id','username']]
    df_P1_users['date'] = df_P1['date'].values
    df_P1_users['id'] = df_P1_users['id'].astype('int8')#MEM

    #Se agrupa segun fechas y se cuentan los IDs para los tweets y los retweets
    df_P1_agrup = df_P1.groupby(df_P1['date'])['id'].count()
    df_P1_retw_agrup = df_P1_quotedTweet.groupby(df_P1_quotedTweet['date'])['id'].count()
    del df_P1 #MEM
    gc.collect() #MEM

    #Se mezclan los dos dataframes segun las fechas y se contabiliza la cantidad de tweets total por fechas, hallando el Top 10 de fechas con más publicaciones
    df_P1_merged = pd.merge(df_P1_agrup, df_P1_retw_agrup, on='date', how='outer', suffixes=('_dftweets', '_dfretweets'))
    df_P1_merged['cantidad_total'] = df_P1_merged['id_dftweets'].fillna(0) + df_P1_merged['id_dfretweets'].fillna(0)
    df_P1_merged = df_P1_merged.sort_values(by='cantidad_total', ascending=False).head(10)
    del df_P1_agrup, df_P1_retw_agrup #MEM
    gc.collect() #MEM

    #Se utiliza la funcion para hallar el usuario que más posteó en las fechas de interes
    P1 = rank_users(df_P1_merged,df_P1_users,df_P1_quotedTweet)
    del df_P1_users,df_P1_quotedTweet,df_P1_merged #MEM
    gc.collect() #MEM

    #Se genera la estructura del output deseado
    output_list_P1 = [(fecha.date(), usuario) for fecha, usuario in zip(P1['Fecha'], P1['Usuario'])]
    del P1 #MEM
    gc.collect()#MEM
    return output_list_P1