import pandas as pd
import numpy as np
'''
We use pandas to take over pyspark's original job since
this is the tool that most of our team are more familiar 
with and easy to implement. 
Using command line to run 'poetry run python ./qf5214_team11/DataProcessing.py'
and the output file will be inside data folder called 'merged_data.csv'
'''
news_data = pd.read_csv('./Data/news_score.csv')
news_data = news_data.iloc[: , 1:]

market_data = pd.read_csv('./Data/2024-04-04+7_US_ALL.csv')
market_data = market_data.iloc[: , 1:]

def data_preprocess(news_data: pd.DataFrame,
                    market_data: pd.DataFrame,
                    output_dir: str)->pd.DataFrame:
        
        market_data['date'] = market_data['Timestamp'].apply(lambda x: x[:10])
        # define dictionary
        match_dict = {'105.NVDA': 'NVIDIA',
                        '105.TSLA': 'Tesla',
                        '105.AMD': 'Advanced Micro Devices',
                        '105.AAPL': 'Apple',
                        '105.MSFT': 'Microsoft',
                        '105.AMZN': 'Amazon.com',
                        '105.META': 'Meta Platforms',
                        '105.SMCI': 'Super Micro Computer',
                        '105.GOOGL': 'Alphabet',
                        '105.MSTR': 'Microstrategy',
                        '105.MU': 'Micron Technology',
                        '105.GOOG': 'Alphabet',
                        '105.AVGO': 'Broadcom',
                        '105.COIN': 'Coinbase Global',
                        '105.INTC': 'Intel',
                        '105.NFLX': 'Netflix',
                        '105.COST': 'Costco Wholesale',
                        '105.ADBE': 'Adobe',
                        '105.QCOM': 'Qualcomm',
                        '105.MRVL': 'Marvell Technology',
                        '105.CSCO': 'Cisco Systems',
                        '105.PANW': 'Palo Alto Networks',
                        '105.AMAT': 'Applied Materials',
                        '105.TXN': 'Texas Instruments',
                        '105.ADI': 'Analog Devices',
                        '105.CMCSA': 'Comcast',
                        '105.LIN': 'Linde',
                        '105.PEP': 'PepsiCo',
                        '105.CEG': 'Constellation Energy',
                        '105.TMUS': 'T-Mobile US',
                        '105.ASML': 'ASML Holding NV',
                        '105.SWAV': 'ShockWave Medical',
                        '105.LULU': 'Lululemon Athletica',
                        '105.AAL': 'American Airlines Group',
                        '105.BKNG': 'Booking',
                        '105.PDD': 'PDD',
                        '105.SBUX': 'Starbucks',
                        '105.ARM': 'Armplc.',
                        '105.MARA': 'Marathon Digital',
                        '105.INTU': 'Intuit',
                        '105.CRWD': 'Crowdstrike',
                        '105.UAL': 'United Airlines',
                        '105.EQIX': 'Equinix',
                        '105.ENPH': 'Enphase Energy',
                        '105.LRCX': 'Lam Research',
                        '105.HON': 'Honeywell International',
                        '105.PYPL': 'PayPal',
                        '105.WDC': 'Western Digital',
                        '105.GILD': 'Gilead Sciences',
                        '105.ALPN': 'Alpine Immune Sciences'}
        
        market_data['stock_queried'] = market_data['Ticker']

        for i, row in market_data.iterrows():
            if row['Ticker'] in match_dict.keys():
                # row['stock_queried'] = match_dict.get(row['Ticker'])
                market_data.loc[i, 'stock_queried'] = match_dict.get(row['Ticker'])

        for i, row in news_data.iterrows():
            news_data.loc[i, 'publishedAt'] = row['publishedAt'][:16]

        market_data['Timestamp'] = market_data['Timestamp'].apply(lambda x: x[:16] if pd.notnull(x) else x)
        news_data.rename({'publishedAt': "Timestamp"}, inplace=True, axis='columns')

        all_data = pd.merge(market_data, news_data, how="outer", on=["stock_queried", "Timestamp"])

        all_data.to_csv(output_dir, index = False)

if __name__ == '__main__':
     data_preprocess(news_data, market_data, './Data/merged_data.csv')