from newsapi import NewsApiClient
import datetime
import json


def get_news(timestamp):
    """
    API used for get all news about American Stock Market in one minute before the assigned timestamp.

    Parameters
    ----------
    timestamp: datetime.datetime()
        the assigned timestamp, this function will query news according to this timestamp, (UTC)

    Returns
    -------
    sign: BOOL
        whether there are news in the assigned time interval
    news_list: list
        JSON response of the list of news which fit our requirements
    """
    newsapi = NewsApiClient(api_key='c0c0f7cbcca2495084941a95b42ccd44')

    previous_timestamp = (timestamp - datetime.timedelta(minutes=1)).strftime("%Y-%m-%dT%H:%M:%S")
    timestamp = timestamp.strftime("%Y-%m-%dT%H:%M:%S")

    all_articles = newsapi.get_everything(
        q='stock',
        language='en',
        from_param=previous_timestamp,
        to=timestamp
    )

    if all_articles['totalResults'] == 0:
        return False, []
    return True, all_articles['articles']


if __name__ == '__main__':
    example_timestamp = datetime.datetime(2024, 4, 8, 18, 20)

    # give you an example of how to query news by giving certain timestamp
    has_news, articles = get_news(example_timestamp)
    print(has_news, articles)
    with open('F:/qf5214_team11/Data/news.jsonl', 'w') as f:
        for item in articles:
            f.write(json.dumps(item) + '\n')

