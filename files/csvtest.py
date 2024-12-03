import pandas as pd
df = pd.read_csv('coin-dance-market-cap-historical.csv',delimiter=";")
#df = df.iloc[:-2].copy()
#df.to_csv('coin-dance-market-cap-historical.csv',index=False,sep=";")
#df = pd.read_csv('coin-dance-market-cap-historical.csv',delimiter=";")
print(df.tail(10))
