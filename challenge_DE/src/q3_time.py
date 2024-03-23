from typing import List, Tuple
import pandas as pd
pd.options.mode.chained_assignment = None
from pandas import json_normalize
import json

def q3_time(file_path: str) -> List[Tuple[str, int]]:

    #Lectura especifica de las columnas de interes
    cols = ['mentionedUsers','quotedTweet']
    data = []
    with open(file_path) as f:
        for line in f:
            doc = json.loads(line)
            lst = [doc['mentionedUsers'], doc['quotedTweet']]
            data.append(lst)

    #Generacion de DataFrame
    df_P3 = pd.DataFrame(data=data, columns=cols)

    #Se expande y se obtienen columnas de interés de columna mentionedUsers
    df_P3_mentionedUsers=df_P3.explode('mentionedUsers')
    df_P3_mentionedUsers = json_normalize(df_P3_mentionedUsers['mentionedUsers'])[['id','username']]
    df_P3_mentionedUsers = df_P3_mentionedUsers[df_P3_mentionedUsers['id'].notnull()]

    #Se expande y se obtienen columnas de interés de columna quotedTweet para hallar los mentionedUsers de los retweets
    df_P3_quotedTweet = json_normalize(df_P3['quotedTweet'])[['mentionedUsers']]
    df_P3_quotedTweet = df_P3_quotedTweet[df_P3_quotedTweet['mentionedUsers'].notnull()]
    df_P3_mentionedUsers_quot=df_P3_quotedTweet.explode('mentionedUsers')
    df_P3_mentionedUsers_quot = json_normalize(df_P3_mentionedUsers_quot['mentionedUsers'])[['id','username']]

    #Se agrupa segun username y se cuentan los IDs para los tweets y los retweets de las menciones
    df_P3_agrup_hist = df_P3_mentionedUsers.groupby(df_P3_mentionedUsers['username'])['id'].count()
    df_P3_retw_hist_agrup = df_P3_mentionedUsers_quot.groupby(df_P3_mentionedUsers_quot['username'])['id'].count()

    #Se mezclan los dos dataframes segun los username y se contabiliza la cantidad de menciones totales, hallando el Top 10 de menciones historicos
    df_P3_merged_menc = pd.merge(df_P3_agrup_hist, df_P3_retw_hist_agrup, on='username', how='outer', suffixes=('_dftweets', '_dfretweets'))
    df_P3_merged_menc['cantidad_total'] = df_P3_merged_menc['id_dftweets'].fillna(0) + df_P3_merged_menc['id_dfretweets'].fillna(0)
    df_P3_merged_menc = df_P3_merged_menc.sort_values(by='cantidad_total', ascending=False).head(10).reset_index()

    #Se acota dataframe final a datos de interes
    P3=df_P3_merged_menc[['username','cantidad_total']]
    P3['cantidad_total'] = P3['cantidad_total'].astype(int)

    #Se genera la estructura del output deseado
    output_list_P3 = [(username, int(cantidad)) for username, cantidad in zip(P3['username'], P3['cantidad_total'])]

    return output_list_P3