"""Main module."""
import os
import time as time_module
from KafkaWorker import KafkaWorker
from config import mysql_user, mysql_password, mysql_hostname, mysql_database_name, mysql_table_name
from createDatabase import create_database

if __name__ == '__main__':

    # Note that market_data was downsampled to only cover 2024-04-05 21:30 to 2024-04-06 04:00
    data_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'data'))
    market_data_path = os.path.join(data_path, '2024-04-04+7_US_ALL.csv')
    news_data_path = os.path.join(data_path, 'top50_traded_stock_news.csv')
    
    # Initialise Kafka worker
    worker = KafkaWorker('2024-04-06 00:00:00', market_data_path, news_data_path)
    worker.start()
    try:
        while True:
            # Just to keep the main thread running
            time_module.sleep(1)
            pass

    except KeyboardInterrupt:
        print('Stopping Kafka Worker.')
        worker.stop()

    # Create DB    
    create_database(mysql_user, mysql_password, mysql_hostname, mysql_database_name, mysql_table_name)