import json
import pandas as pd
import numpy as np
import io
import logging
import akshare as ak
import datetime


class GetData:
    """Get the data from Akshare API
    Parameters
    ----------
    output_format: str, optional (default='json')
        Specify the output format. Available formats: 'json' or 'csv'.
    """

    def __init__(self, output_format='csv'):
        self.output_format = output_format

        assert (output_format == 'json' or output_format == 'csv'), '{} format is not supported' \
            .format(output_format)

    def get_akshare_data(self, timestamp, function=None, interval=None, symbol=None):
        """
        Get the market data from Akshare API, with INTRADAY function returns the latest minute bar,
        with INTRADAY_100 or HIST_100 it returns 100 latest data points, otherwise the time series
        covering longest available historical daily data is returned.

        Parameters
        ----------
        function: str, optional (default=None)
            Specify the API function to use, for instance: INTRADAY, INTRADAY_100, HIST_100, ALL_HIST.
        symbol: str, optional (default=None)
            The name of the US stock. For example: 105.CAC.
        interval: str, optional (default=None)
            Time interval between two consecutive data points in the time series. Only applicable
            for HIST_100. For example: daily, weekly, monthly
        timestamp: datetime.datetime()
            Timestamp of real-time data (UTC+8).
        """
        if function == 'INTRADAY':
            previous_timestamp = (timestamp - datetime.timedelta(minutes=1)).strftime("%Y-%m-%d %H:%M:%S")
            df = ak.stock_us_hist_min_em(symbol, start_date=previous_timestamp, end_date=previous_timestamp)
            df.columns = ['datetime', 'open', 'close', 'high', 'low', 'volume', 'volume(price)', 'latest_px']
            return df.iloc[0]
        elif function == 'INTRADAY_100':
            back100_ts = (timestamp - datetime.timedelta(minutes=100)).strftime("%Y-%m-%d %H:%M:%S")
            back1_ts = (timestamp - datetime.timedelta(minutes=1)).strftime("%Y-%m-%d %H:%M:%S")
            df = ak.stock_us_hist_min_em(symbol, start_date=back100_ts, end_date=back1_ts)
            df.columns = ['datetime', 'open', 'close', 'high', 'low', 'volume', 'volume(price)', 'latest_px']
            return df
        elif function == 'HIST_100':
            df = ak.stock_us_hist(symbol, end_date=timestamp.strftime("%Y-%m-%d %H:%M:%S"), period=interval)
            if len(df) > 101:
                return df[-100:]
            else:
                return df
        elif function == 'ALL_HIST':
            df = ak.stock_us_hist(symbol, end_date=timestamp.strftime("%Y-%m-%d %H:%M:%S"), period=interval)
            return df


if __name__ == '__main__':
    # Here I provide some examples
    example_ts = datetime.datetime(2024, 4, 1, 23, 38)
    example_symbol = '105.CAC'

    # 1. INTRADAY: give a timestamp and symbol, return a data point
    df = GetData().get_akshare_data(timestamp=example_ts, symbol=example_symbol, function='INTRADAY')
    print(df)

    # 2. INTRADAY_100: give a timestamp and symbol, return 100 latest data points until the assigned timestamp
    df2 = GetData().get_akshare_data(timestamp=example_ts, symbol=example_symbol, function='INTRADAY_100')
    print(df2)

    # 3. HIST_100: give a timestamp, symbol and the data interval, return 100 latest data points until the assigned timestamp
    df3 = GetData().get_akshare_data(timestamp=example_ts, symbol=example_symbol, function='HIST_100', interval='weekly')
    print(df3)

    # 4. ALL_HIST: give a timestamp, symbol and the data interval, return all available historical data
    df4 = GetData().get_akshare_data(timestamp=example_ts, symbol=example_symbol, function='ALL_HIST', interval='monthly')
    print(df4)





