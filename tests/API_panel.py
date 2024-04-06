import pandas as pd
from tqdm import tqdm
import akshare as ak


us_spot = ak.stock_us_spot_em()
us_stock_list = us_spot['代码'].unique()
print(us_stock_list)

total_df = ak.stock_us_hist_min_em(symbol=us_stock_list[0], start_date='2024-04-01', end_date='2024-04-06')
total_df['ticker'] = [us_stock_list[500]]*len(total_df)
print(total_df)
for ticker in tqdm(us_stock_list[501:1000]):
    ts_df = ak.stock_us_hist_min_em(symbol=ticker, start_date='2024-04-01', end_date='2024-04-06')
    ts_df['ticker'] = [ticker]*len(ts_df)
    total_df = pd.concat([total_df, ts_df], ignore_index=True)
print(total_df)
total_df.to_csv('/Data/us_stock_20240401_20240406_2.csv')
