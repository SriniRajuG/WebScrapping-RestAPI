import json
import os
from collections import namedtuple
import time
import logging
import sqlite3
import sys

import requests
from requests_oauthlib import OAuth1
import requests_cache

import conf

# Logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler = logging.FileHandler(conf.log_file)
file_handler.setLevel(logging.WARNING)
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)
stream_handler = logging.StreamHandler()
stream_handler.setLevel(logging.DEBUG)
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)


def get_db_connection():
    """
    Creates a connection to sqlite database and returns a connection instance.
    """
    try:
        conn = sqlite3.connect(conf.db_file)
    except sqlite3.Error as err:
        logger.exception("Error while connecting to database")
        raise err
    else:
        return conn


def get_sql_queries():
    """
    Define sql queries and return a dictionary whose values are query strings.
    """
    queries = dict()
    queries['create_trend'] = """
       CREATE TABLE IF NOT EXISTS trend (
          trend_text TEXT NOT NULL ,
          query TEXT NOT NULL ,
          status_volume INTEGER ,
          PRIMARY KEY (trend_text)
       );
    """
    queries['create_status'] = """
       CREATE TABLE IF NOT EXISTS status (
          status_id INTEGER NOT NULL ,
          text TEXT NOT NULL ,
          creation_date TEXT ,
          language TEXT ,
          retweet_count INTEGER ,
          favorite_count INTEGER ,
          user_id INTEGER NOT NULL ,
          PRIMARY KEY (status_id) ,
          FOREIGN KEY (user_id) REFERENCES user (user_id) 
             ON DELETE RESTRICT 
             ON UPDATE RESTRICT 
       );
    """
    queries['create_user'] = """
       CREATE TABLE IF NOT EXISTS user (
           user_id INTEGER NOT NULL ,
           name TEXT,
           screen_name TEXT,
           creation_date TEXT ,
           description TEXT ,
           is_verified INTEGER ,
           friend_count INTEGER ,
           follower_count INTEGER ,
           favorite_count INTEGER ,
           status_count INTEGER ,
           PRIMARY KEY (user_id)
       );
    """
    # queries['create_location'] = """
    #    CREATE TABLE IF NOT EXISTS location (
    #       location_id INTEGER NOT NULL ,
    #       text NOT NULL ,
    #       PRIMARY KEY (location_id)
    #    );
    # """
    queries['create_status_trend'] = """
       CREATE TABLE IF NOT EXISTS status_trend (
          status_id INTEGER ,
          trend_text TEXT ,
          PRIMARY KEY (status_id, trend_text) ,
          FOREIGN KEY (status_id) REFERENCES status (status_id) 
             ON UPDATE RESTRICT 
             ON DELETE RESTRICT ,
          FOREIGN KEY (trend_text) REFERENCES trend (trend_text) 
             ON UPDATE RESTRICT 
             ON DELETE RESTRICT
       );
    """
    return queries


def create_table(conn, query):
    try:
        with conn:
            conn.execute(query)
    except sqlite3.Error:
        logger.exception("sqlite exception")
        sys.exit()


def get_auth():
    """Create and return an instance of OAuth1"""
    access_token = os.environ.get('twitter_app_access_token')
    access_token_secret = os.environ.get('twitter_app_access_token_secret')
    consumer_key = os.environ.get('twitter_app_consumer_key')
    consumer_key_secret = os.environ.get('twitter_app_consumer_key_secret')
    auth = OAuth1(
        client_key=consumer_key,
        client_secret=consumer_key_secret,
        resource_owner_key=access_token,
        resource_owner_secret=access_token_secret,
    )
    return auth


def insert_trend(conn, trend):
    query = """
       INSERT INTO trend (trend_text, query, status_volume) VALUES (?, ?, ?);
    """
    try:
        with conn:
            conn.execute(
                query,
                (trend.trend_text, trend.query, trend.status_volume)
            )
    except sqlite3.Error:
        logger.info(f"duplicate trend ignored: {trend.trend_text}")


def gen_trends_response(auth):
    woeids = {
        'new_york': 2459115,
        'los_angeles': 2442047,
        'chicago': 2379574,
        'houston': 2424766,
        'phoenix': 2471390,
        'philadelphia': 2471217,
        'dallas': 2388929,
        'san_diego': 2487889,
        'detroit': 2391585,
        'jacksonville': 2428344,
        'columbus': 2383660,
        'milwaukee': 2451822,
        'baltimore': 2358820,
        'boston': 2367105,
        'seattle': 2490383,
        'las_vegas': 2436704,
    }
    url = 'https://api.twitter.com/1.1/trends/place.json'
    for woeid in woeids.values():
        params = {'id': str(woeid)}
        try:
            response = requests.get(url, params=params, auth=auth, timeout=1)
            response.raise_for_status()
        except requests.exceptions.Timeout:
            logger.warning("Timeout Exception: retying after 15 minutes.")
            time.sleep(900)  # 15 minutes
            continue
            # Todo: Use the same woeid, instead of the next one in the loop
            # Todo: Send email if Timeout exception happens
        except requests.exceptions.RequestException:
            logger.exception("Request exception")
            sys.exit(1)
        response = json.loads(response.content)
        logger.info("Sleeping for 5 minutes, before sending "
                    "the next request for trends.")
        yield response
        time.sleep(300)  # 5 minutes


def get_trends(conn):
    """
    Query the table "trend" from the database and yield a trend query string.
    """
    query = """
       SELECT trend_text, query FROM trend;
    """
    with conn:
        curs = conn.cursor()
        curs.execute(query)
        rows = curs.fetchall()
        conn.commit()
    return rows


def gen_status(auth, trend_query):
    """
    Makes HTTP requests for statuses. Yields a dictionary with a status.
    """
    base_url = 'https://api.twitter.com/1.1/search/tweets.json'
    params = {
        'q': trend_query,
        'lang': 'en',
        'result_type': 'mixed',
        'include_entities': 'true',
        'count': conf.max_status_per_request,
        'tweet_mode': 'extended',
    }
    first_search_request = True
    response_counter = 0
    while True:
        try:
            if first_search_request:
                response = requests.get(
                    url=base_url,
                    params=params,
                    auth=auth,
                    timeout=1,
                )
            else:
                url = base_url + response['search_metadata']['next_results']
                params = {'tweet_mode': 'extended'}
                response = requests.get(
                    url=url,
                    auth=auth,
                    params=params,
                    timeout=1,
                )
            response.raise_for_status()
        except requests.exceptions.Timeout:
            logger.warning("Timeout Exception: retying after 15 minutes.")
            time.sleep(900)  # 15 minutes
            continue
            # Todo: Use the same woeid, instead of the next one in the loop
            # Todo: Send email if Timeout exception happens
        except requests.exceptions.RequestException:
            logger.exception("Request exception")
            sys.exit(1)
        response = json.loads(response.content)
        status_per_response_counter = 0
        for status in response['statuses']:
            status_per_response_counter += 1
            yield status
        first_search_request = False
        response_counter += 1
        time.sleep(6)
        logger.info(f"Received {response_counter} responses for statuses.")
        if status_per_response_counter < conf.max_status_per_request * 0.7 or \
                response_counter >= conf.status_max_requests:
            break


def insert_user(conn, user):
    query = """
       INSERT INTO user (
          user_id,
          name,
          screen_name,
          creation_date,
          description,
          is_verified,
          friend_count,
          follower_count,
          favorite_count,
          status_count
       ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
    """
    try:
        with conn:
            conn.execute(
                query,
                (
                    user.user_id,
                    user.name,
                    user.screen_name,
                    user.creation_date,
                    user.description,
                    user.is_verified,
                    user.friend_count,
                    user.follower_count,
                    user.favorite_count,
                    user.status_count,
                )
            )
    except sqlite3.Error as err:
        # logger.info(f"duplicate user ignored: {user.user_id}")
        logger.info(err)


def insert_status(conn, status):
    query = """
       INSERT INTO status (
          status_id,
          text,
          creation_date,
          language,
          retweet_count, 
          favorite_count,
          user_id
       ) VALUES (?, ?, ?, ?, ?, ?, ?);
    """
    try:
        with conn:
            conn.execute(
                query,
                (
                    status.status_id,
                    status.text,
                    status.creation_date,
                    status.language,
                    status.retweet_count,
                    status.favorite_count,
                    status.user_id,
                )
            )
    except sqlite3.Error as err:
        # logger.info(f"duplicate status ignored: {status.status_id}")
        logger.info(err)


def insert_status_trend(conn, status_trend):
    query = """
       INSERT INTO status_trend (
          status_id,
          trend_text
       ) VALUES (?, ?);
    """
    try:
        with conn:
            conn.execute(
                query,
                (status_trend.status_id, status_trend.trend_text)
            )
    except sqlite3.Error as err:
        logger.info(err)
        # logger.info(f"duplicate status_trend ignored: "
        #             f"{status_trend.status_id, status_trend.trend_text}")


def get_entities(status_, trend_text, Status, User, StatusTrend):
    user_ = status_['user']
    user = User(
        user_id=user_['id'],
        name=user_['name'],
        screen_name=user_['screen_name'],
        creation_date=user_['created_at'],
        description=user_['description'],
        is_verified=user_['verified'],
        friend_count=user_['friends_count'],
        follower_count=user_['followers_count'],
        favorite_count=user_['favourites_count'],
        status_count=user_['statuses_count'],
    )
    status = Status(
        status_id=status_['id'],
        text=status_['full_text'],
        creation_date=status_['created_at'],
        language=status_['lang'],
        retweet_count=status_['retweet_count'],
        favorite_count=status_['favorite_count'],
        user_id=user.user_id,
    )
    status_trend = StatusTrend(
        status_id=status.status_id,
        trend_text=trend_text,
    )
    return user, status, status_trend


def main():
    requests_cache.install_cache()
    queries = get_sql_queries()
    conn = get_db_connection()
    # create_table(conn, queries['create_trend'])
    create_table(conn, queries['create_user'])
    create_table(conn, queries['create_status'])
    create_table(conn, queries['create_status_trend'])

    auth = get_auth()

    # # Trend
    # Trend = namedtuple('Trend', 'trend_text query status_volume')
    # for response in gen_trends_response(auth):
    #     for trnd in response[0]['trends']:
    #         trend = Trend(
    #             trend_text=trnd['name'],
    #             query=trnd['query'] ,
    #             status_volume=trnd['tweet_volume'],
    #         )
    #         insert_trend(conn=conn, trend=trend)

    user_fields = [
        'user_id',
        'name',
        'screen_name',
        'creation_date',
        'description',
        'is_verified',
        'friend_count',
        'follower_count',
        'favorite_count',
        'status_count',
    ]
    User = namedtuple('User', user_fields)
    status_fields = [
        'status_id',
        'text',
        'creation_date',
        'language',
        'retweet_count',
        'favorite_count',
        'user_id'
    ]
    Status = namedtuple('Status', status_fields)
    StatusTrend = namedtuple('StatusTrend', 'status_id trend_text')

    trends = get_trends(conn)
    for trend_text, trend_query in trends:
        trend_query = '#' + trend_query
        for status_ in gen_status(auth, trend_query):
            user, status, status_trend = get_entities(
                status_,
                trend_text,
                Status,
                User,
                StatusTrend
            )
            insert_user(conn, user)
            insert_status(conn, status)
            insert_status_trend(conn, status_trend)
    conn.close()


if __name__ == '__main__':
    main()
