import pandas as pd
import numpy as np
pd.set_option('display.max_rows', 500) 

file_path = "farmers-protest-tweets-2021-2-4.json"
#file_path2 = "C:\\Users\\JuanJo\\Desktop\\LATAM\\Challenge\\farmers-protest-tweets-2021-2-4.json"

df=pd.read_json(file_path, lines=True)
#print(df.head(5))

#print(df.columns)
