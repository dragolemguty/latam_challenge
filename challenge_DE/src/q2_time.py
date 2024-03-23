from typing import List, Tuple
import pandas as pd
pd.options.mode.chained_assignment = None
from pandas import json_normalize
import json
import emoji
from collections import Counter

def q2_time(file_path: str) -> List[Tuple[str, int]]:
    cols = ['content','quotedTweet']
    data = []
    with open(file_path) as f:
        for line in f:
            doc = json.loads(line)
            lst = [doc['content'], doc['quotedTweet']]
            data.append(lst)
    df_P2 = pd.DataFrame(data=data, columns=cols)

    df_P2_quotedTweet = json_normalize(df_P2['quotedTweet'])[['content']]
    df_P2_quotedTweet = df_P2_quotedTweet[df_P2_quotedTweet['content'].notnull()]

    emojis_P2 = []
    for i in df_P2['content']:
        emojis_P2 += [c for c in i if c in emoji.EMOJI_DATA]
    conteo_emojis = Counter(emojis_P2)
    df_P2_emojis_twet = pd.DataFrame(list(conteo_emojis.items()), columns=['emoji', 'cantidad'])
    df_P2_emojis_twet.set_index('emoji', inplace=True)

    emojis_P2_ret = []
    for i in df_P2_quotedTweet['content']:
        emojis_P2_ret += [c for c in i if c in emoji.EMOJI_DATA]
    conteo_emojis = Counter(emojis_P2_ret)
    df_P2_emojis_retwet = pd.DataFrame(list(conteo_emojis.items()), columns=['emoji', 'cantidad'])
    df_P2_emojis_retwet.set_index('emoji', inplace=True)

    df_P2_merged_emoji = pd.merge(df_P2_emojis_twet, df_P2_emojis_retwet, on='emoji', how='outer', suffixes=('_dftweets', '_dfretweets'))
    df_P2_merged_emoji ['cantidad_total'] = df_P2_merged_emoji ['cantidad_dftweets'].fillna(0) + df_P2_merged_emoji ['cantidad_dfretweets'].fillna(0)
    df_P2_merged_emoji = df_P2_merged_emoji.sort_values(by='cantidad_total', ascending=False).head(10).reset_index()

    P2_short=df_P2_merged_emoji[['emoji','cantidad_total']]
    P2_short['cantidad_total'] = P2_short['cantidad_total'].astype(int)
    output_list_P2 = [(emo, int(cantidad)) for emo, cantidad in zip(P2_short['emoji'], P2_short['cantidad_total'])]

    return output_list_P2