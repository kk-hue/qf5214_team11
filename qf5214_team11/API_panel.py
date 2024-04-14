import pandas as pd
from tqdm import tqdm
import akshare as ak


us_spot = ak.stock_us_spot_em()
equity = us_spot[us_spot['代码'].str[:3] == '105'].dropna(how='any')
us_stock_list = equity['代码'].unique()
print(us_stock_list)

total_df = ak.stock_us_hist_min_em(symbol=us_stock_list[0], start_date='2024-04-04', end_date='2024-04-10')
total_df['ticker'] = [us_stock_list[0]]*len(total_df)
print(total_df)
for ticker in tqdm(us_stock_list[1:]):
    ts_df = ak.stock_us_hist_min_em(symbol=ticker, start_date='2024-04-04', end_date='2024-04-10')
    ts_df['ticker'] = [ticker]*len(ts_df)
    total_df = pd.concat([total_df, ts_df], ignore_index=True)
print(total_df)
total_df.columns = ['datetime', 'open', 'close', 'high', 'low', 'volume', 'volume_price', 'last_price', 'ticker']
total_df.to_csv('F:/qf5214_team11/Data/us_stock_20240404_20240410.csv')
