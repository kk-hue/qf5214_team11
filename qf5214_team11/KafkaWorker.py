from IPython.display import display
from kafka import KafkaProducer, KafkaConsumer
from io import StringIO
import json
import time as time_module
import pandas as pd
from datetime import datetime, time, timedelta
from threading import Thread, Event


def _validate_timestamp_format(timestamp):
    # timestamps should be in the format: YYYY-MM-DD HH:MM:SS
    # e.g. 2024-04-10 21:31:00
    try:
        datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S')
        return True
    except ValueError:
        return False


class KafkaWorker:
    KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
    KAFKA_TOPIC1 = 'market-data'
    KAFKA_TOPIC2 = 'news-data'

    def __init__(self, timestamp, market_data_csv, news_data_csv):
        if not _validate_timestamp_format(timestamp):
            raise ValueError("Invalid timestamp format. Please use 'YYYY-MM-DD HH:MM:SS'.")

        self.timestamp = timestamp

        # Load stock market data (5 traded days on 2024-04-04, 04-05, 04-06, 04-09, 04-10)
        self.market_data = pd.read_csv(market_data_csv, index_col=0)
        # Load news data for selected stocks (Any news arrived between 2024-04-04 and 2024-04-10)
        self.news_data = pd.read_csv(news_data_csv, index_col=0)
        self.news_data['publishedAt'] = pd.to_datetime(self.news_data['publishedAt']).dt.tz_localize(None)

        # Set up producer
        self.producer = KafkaProducer(
                                        bootstrap_servers=self.KAFKA_BOOTSTRAP_SERVERS,
                                        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                                        api_version=(0, 10, 1)
        )

        # Set up consumer
        self.consumer = KafkaConsumer(
                                        self.KAFKA_TOPIC1,
                                        self.KAFKA_TOPIC2,
                                        bootstrap_servers=self.KAFKA_BOOTSTRAP_SERVERS,
                                        auto_offset_reset='latest',
                                        enable_auto_commit=True,
                                        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                                        api_version=(0, 10, 1)
        )

        # Threads
        self.producer_thread = Thread(target=self.run_producer)
        self.consumer_thread = Thread(target=self.run_consumer)

        # Control the threads
        self.stop_event = Event()

    def run_producer(self):
        ts = datetime.strptime(self.timestamp, "%Y-%m-%d %H:%M:%S")

        # Helper function for producing news at a particular minute
        get_minute_news = lambda data, ts_str: data[data['publishedAt'].dt.strftime('%Y-%m-%d %H:%M') == ts_str]

        # Check if the supplied timestamp falls in 2130 UTC+8 to 0400 UTC+8
        if ts.time() >= time(21, 30) or ts.time() <= time(4, 0):

            while not self.stop_event.is_set():
                print(ts)

                if ts.time().hour == 4 and ts.time() > time(4, 0):
                    print('Market closed.')
                    break

                ts_str = ts.strftime('%Y-%m-%d %H:%M:%S')
                mkt_ts_df = self.market_data[self.market_data['Timestamp'] == ts_str]

                # Produce market data
                self.producer.send(topic=self.KAFKA_TOPIC1,
                                   value=mkt_ts_df.to_json(orient='records', date_format='iso'))
                print(f"Produced data to topic {self.KAFKA_TOPIC1}")

                # Produce news data
                # This involves an additional step of checking whether any news arrived at this timestamp
                ts_str = ts.strftime('%Y-%m-%d %H:%M')  # we only need to match up to minute
                if get_minute_news(self.news_data, ts_str).shape[0] > 0:
                    news_ts_df = get_minute_news(self.news_data, ts_str)
                    self.producer.send(topic=self.KAFKA_TOPIC2,
                                       value=news_ts_df.to_json(orient='records', date_format='iso'))
                    print(f"Produced data to topic {self.KAFKA_TOPIC2}")

                time_module.sleep(10)  # Sleep for 1 minute as we are dealing with minute bar data

                # Increment minute
                ts += timedelta(minutes=1)

        else:
            print("Outside trading hours.")

    def run_consumer(self):

        while True:

            message = self.consumer.poll(timeout_ms=10000)  # Adjust timeout as needed
            if not message and self.stop_event.is_set():
                self.consumer.close()
                print('Consumer Closed.')
                break

            for tp, records in message.items():

                if tp.topic == self.KAFKA_TOPIC1:  # market data
                    market_ts_data = pd.read_json(StringIO(records[0].value), orient='records')
                    print(f"Consumed data from topic {self.KAFKA_TOPIC1}")
                    display(market_ts_data)

                if tp.topic == self.KAFKA_TOPIC2: # news data
                    news_ts_data = pd.read_json(StringIO(records[0].value), orient='records')
                    print(f"Consumed data from topic {self.KAFKA_TOPIC2}")
                    display(news_ts_data)

        # === IF CONSUMER WAS WRITTEN WITH THE BELOW FOR MESSAGE-LOOP IT CANNOT SHUT DOWN GRACEFULLY ===
        # while not self.stop_event.is_set():
        #
        #     for message in self.consumer:
        #
        #         if self.stop_event.is_set():
        #             print('consumer instructed to break')
        #             break
        #
        #         if message.topic == self.KAFKA_TOPIC1:  # market data
        #             market_ts_data = pd.read_json(StringIO(message.value), orient='records')
        #             print(f"Consumed data from topic {self.KAFKA_TOPIC1}")
        #             display(market_ts_data)
        #
        #         if message.topic == self.KAFKA_TOPIC2:  # news data
        #             news_ts_data = pd.read_json(StringIO(message.value), orient='records')
        #             print(f"Consumed data from topic {self.KAFKA_TOPIC2}")
        #             display(news_ts_data)

    def start(self):
        self.producer_thread.start()
        self.consumer_thread.start()

    # Signal both threads to stop by setting the event.
    # Close the consumer connection, which should prompt the consumer thread to finish up if it's waiting for messages.
    # Wait for the producer thread to finish.
    # Wait for the consumer thread to finish.
    def stop(self):
        self.stop_event.set()
        print('Stop Event Triggered.')
        self.producer.close()
        print('Producer Closed.')
        self.producer_thread.join()
        self.consumer_thread.join()
        print('Kafka Worker Stopped.')


# Example usage
if __name__ == "__main__":
    worker = KafkaWorker('2024-04-06 00:00:00', '2024-04-04+7_US_ALL.csv', 'top50_traded_stock_news.csv')
    worker.start()
    try:
        while True:
            # Just to keep the main thread running
            time_module.sleep(1)
            pass

    except KeyboardInterrupt:
        print('Stopping Kafka Worker.')
        worker.stop()
