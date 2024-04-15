import pandas as pd

df = pd.read_csv("./merged_data.csv", low_memory=False)

df['close_MA'] = df.groupby('Ticker')['Close'].transform(lambda x: x.rolling(window=20, min_periods=1).mean())

df['StdDev'] = df.groupby('Ticker')['Close'].transform(lambda x: x.rolling(window=20, min_periods=1).std())

df['UpperBand'] = df['close_MA'] + df['StdDev']

df['LowerBand'] = df['close_MA'] - df['StdDev']

df.to_csv("./merged_data_2.csv")
