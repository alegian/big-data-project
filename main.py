from datetime import datetime

from cassandra import ConsistencyLevel, OperationTimedOut
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster, ExecutionProfile, EXEC_PROFILE_DEFAULT
from os.path import exists
import os
import pandas as pd
import time
import ast
import re
from dotenv import load_dotenv
load_dotenv()
pd.options.mode.chained_assignment = None  # default='warn'


def db_connect():
    cloud_config = {
        'secure_connect_bundle': './secure-connect-bigdataproject.zip'
    }
    profile = ExecutionProfile(
        request_timeout=100000
    )
    auth_provider = PlainTextAuthProvider(os.environ.get('DB_USER'), os.environ.get('DB_SECRET'))
    cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider, execution_profiles={EXEC_PROFILE_DEFAULT: profile})

    return cluster.connect('main')


def db_setup():
    session.execute("""
    CREATE TABLE IF NOT EXISTS main.movie_ratings (
        partition_key int,
        movie_title text,
        avg_rating float,
        PRIMARY KEY (partition_key, avg_rating, movie_title)
    ) WITH CLUSTERING ORDER BY (avg_rating DESC, movie_title DESC);
    """)
    session.execute("""
        CREATE TABLE IF NOT EXISTS main.movie_details (
            movie_title text,
            movie_genres text, 
            avg_rating float,
            tag text,
            tag_frequency int,
            PRIMARY KEY (movie_title, tag_frequency, tag)
        ) WITH CLUSTERING ORDER BY (tag_frequency DESC, tag DESC);
    """)
    session.execute("""
        CREATE TABLE IF NOT EXISTS main.movie_genres (
            movie_title text,
            movie_genre text, 
            movie_year int,
            PRIMARY KEY (movie_genre, movie_year, movie_title)
        ) WITH CLUSTERING ORDER BY (movie_year DESC, movie_title DESC);
    """)
    session.execute("""
        CREATE TABLE IF NOT EXISTS main.movie_titles (
            movie_title text,
            movie_title_split list<text>,
            PRIMARY KEY (movie_title)
        );
    """)
    session.execute("""
        CREATE TABLE IF NOT EXISTS main.movie_tags (
            movie_title text,
            avg_rating float,
            tag text,
            PRIMARY KEY (tag, avg_rating, movie_title)
        ) WITH CLUSTERING ORDER BY (avg_rating DESC, movie_title DESC);
    """)


def db_insert(table_name, df, consistency):
    print(f'\t\tMass inserting 1000 rows into {table_name}. This might take some minutes...')
    start = time.time()

    for row in df.values.tolist():
        # variable table name, variable number of columns
        insert_query = session.prepare(f"""
            INSERT INTO main.{table_name} ({','.join(df.columns.values.tolist())}) 
            VALUES ({','.join(['?']*len(df.columns))})
        """)
        insert_query.consistency_level = consistency
        session.execute(insert_query, row)

    end = time.time()
    print(f'\t\tInsert 1000 rows into {table_name} took {round(end-start, 4)} seconds')


def db_truncate(table_name):
    try:
        query = session.prepare(f"""
            TRUNCATE main.{table_name}
        """)
        session.execute(query)
    except OperationTimedOut:
        print('truncate timed out')


def db_truncate_all():
    print(f'Erasing old data...')
    db_truncate('movie_ratings')
    db_truncate('movie_details')
    db_truncate('movie_genres')
    db_truncate('movie_tags')


start_date = datetime.strptime('2015-01-01', '%Y-%m-%d')
end_date = datetime.strptime('2015-01-15', '%Y-%m-%d')


def is_valid_date(date_str):
    rating = datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S')
    return start_date <= rating <= end_date


def create_movie_ratings():
    if not exists('movie_ratings.csv'):
        # load data
        movie_df = pd.read_csv('movie.csv')
        rating_df = pd.read_csv('rating.csv')

        # generate table data
        rating_df = rating_df[rating_df['timestamp'].apply(is_valid_date)]
        rating_df = rating_df[['movieId', 'rating']] \
            .groupby('movieId') \
            .mean() \
            .round({'rating': 2})
        movie_ratings_df = rating_df\
            .join(movie_df[['movieId', 'title']].set_index('movieId'), on='movieId')

        # rename for compatibility
        movie_ratings_df = movie_ratings_df \
            .reset_index() \
            .drop(columns=['movieId']) \
            .rename(columns={
                'title': 'movie_title',
                'rating': 'avg_rating'
            })

        # constant partition key, allows for global sorting
        movie_ratings_df['partition_key'] = 0

        # return the first 1000 rows, and create a CSV with the rest
        print('\tcreating movie_ratings.csv...')
        movie_ratings_df.to_csv('movie_ratings.csv', index=False)
        sample = movie_ratings_df.head(1000)
        return sample
    else:
        movie_ratings_df = pd.read_csv('movie_ratings.csv')
        sample = movie_ratings_df.head(1000)
        return sample


def create_movie_details():
    if not exists('movie_details.csv'):
        # load data
        movie_df = pd.read_csv('movie.csv')
        rating_df = pd.read_csv('rating.csv')
        tag_df = pd.read_csv('tag.csv')

        # generate table data
        avg_ratings_df = rating_df[['movieId', 'rating']] \
            .groupby('movieId') \
            .mean()\
            .round({'rating': 2})
        tag_freq_df = tag_df[['movieId', 'tag']]
        tag_freq_df['tag_frequency'] = tag_freq_df\
            .groupby(['movieId', 'tag'])['movieId']\
            .transform('count')
        tag_freq_df = tag_freq_df.dropna().drop_duplicates()
        tag_freq_df['tag_frequency'] = tag_freq_df['tag_frequency'].astype(int)
        movie_details_df = avg_ratings_df \
            .join(movie_df[['movieId', 'title', 'genres']].set_index('movieId'), on='movieId')
        movie_details_df = movie_details_df \
            .join(tag_freq_df.set_index('movieId'), how='inner', on='movieId')

        # rename for compatibility
        movie_details_df = movie_details_df\
            .reset_index()\
            .drop(columns=['movieId']) \
            .rename(columns={
                'rating': 'avg_rating',
                'title': 'movie_title',
                'genres': 'movie_genres'
            })

        # return the first 1000 rows, and create a CSV with the rest
        print('\tcreating movie_details.csv...')
        movie_details_df.to_csv('movie_details.csv', index=False)
        sample = movie_details_df.head(1000)
        return sample
    else:
        movie_details_df = pd.read_csv('movie_details.csv')
        sample = movie_details_df.head(1000)
        return sample


# string hack to get movie year that appears in the form (1999) at the end of a string title
def extract_year_from_title(title):
    # last occurrence of '('
    start_idx = title.rfind('(') + 1
    # last occurrence of ')'
    end_idx = title.rfind(')')

    try:
        return int(title[start_idx:end_idx])
    except ValueError:
        return 0


def create_movie_genres():
    if not exists('movie_genres.csv'):
        # load data
        movie_df = pd.read_csv('movie.csv')

        # create a row for each genre-title pair
        movie_genres_df = pd.DataFrame(movie_df.genres.str.split('|').tolist(), index=movie_df.title).stack()
        movie_genres_df = movie_genres_df.reset_index([0, 'title'])

        # rename columns for compatibility
        movie_genres_df.columns = ['movie_title', 'movie_genre']

        # smart substring & typecast to extract year
        movie_genres_df['movie_year'] = movie_genres_df['movie_title'].apply(extract_year_from_title)

        # return the first 1000 rows, and create a CSV with the rest
        print('\tcreating movie_genres.csv...')
        movie_genres_df.to_csv('movie_genres.csv', index=False)
        sample = movie_genres_df.head(1000)
        return sample
    else:
        movie_genres_df = pd.read_csv('movie_genres.csv')
        sample = movie_genres_df.head(1000)
        return sample


def clean_and_split_title(title):
    return list(filter(None, re.sub(r'[^A-Za-z0-9 ]+', '', title).split(' ')))


def create_movie_titles():
    if not exists('movie_titles.csv'):
        # load data
        movie_df = pd.read_csv('movie.csv')

        # create a row for each title-split_title pair
        movie_titles_df = pd.DataFrame()
        movie_titles_df['movie_title'] = movie_df.title
        movie_titles_df['movie_title_split'] = movie_df.title.apply(clean_and_split_title)

        # rename columns for compatibility
        movie_titles_df.columns = ['movie_title', 'movie_title_split']


        # return the first 1000 rows, and create a CSV with the rest
        print('\tcreating movie_titles.csv...')
        movie_titles_df.to_csv('movie_titles.csv', index=False)
        sample = movie_titles_df.head(1000)
        return sample
    else:
        movie_titles_df = pd.read_csv('movie_titles.csv')
        movie_titles_df['movie_title_split'] = movie_titles_df['movie_title_split'].apply(ast.literal_eval)
        sample = movie_titles_df.head(1000)
        return sample


def create_movie_tags():
    if not exists('movie_tags.csv'):
        # load data
        movie_df = pd.read_csv('movie.csv')
        rating_df = pd.read_csv('rating.csv')
        tag_df = pd.read_csv('tag.csv')

        # generate table data
        avg_ratings_df = rating_df[['movieId', 'rating']] \
            .groupby('movieId') \
            .mean()\
            .round({'rating': 2})
        tag_df = tag_df[['movieId', 'tag']].dropna().drop_duplicates()
        movie_tags_df = avg_ratings_df \
            .join(movie_df[['movieId', 'title']].set_index('movieId'), on='movieId')
        movie_tags_df = movie_tags_df \
            .join(tag_df.set_index('movieId'), how='inner', on='movieId')

        # rename for compatibility
        movie_tags_df = movie_tags_df\
            .reset_index()\
            .drop(columns=['movieId']) \
            .rename(columns={
                'rating': 'avg_rating',
                'title': 'movie_title'
            })

        # return the first 1000 rows, and create a CSV with the rest
        print('\tcreating movie_tags.csv...')
        movie_tags_df.to_csv('movie_tags.csv', index=False)
        sample = movie_tags_df.head(1000)
        return sample
    else:
        movie_tags_df = pd.read_csv('movie_tags.csv')
        sample = movie_tags_df.head(1000)
        return sample


# creates data for all 3 tables, and inserts it all into CassandraDB
def create_and_insert_data():
    print(f'Generating data...')
    movie_ratings_sample = create_movie_ratings()
    movie_details_sample = create_movie_details()
    movie_genres_sample = create_movie_genres()
    movie_titles_sample = create_movie_titles()
    movie_tags_sample = create_movie_tags()

    print('Testing insert times for different consistency levels:')
    print('\tConsistency TWO:')
    db_insert('movie_ratings', movie_ratings_sample, ConsistencyLevel.TWO)
    db_insert('movie_details', movie_details_sample, ConsistencyLevel.TWO)
    db_insert('movie_genres', movie_genres_sample, ConsistencyLevel.TWO)
    db_insert('movie_titles', movie_titles_sample, ConsistencyLevel.TWO)
    db_insert('movie_tags', movie_tags_sample, ConsistencyLevel.TWO)
    print('\tConsistency QUORUM:')
    db_insert('movie_ratings', movie_ratings_sample, ConsistencyLevel.QUORUM)
    db_insert('movie_details', movie_details_sample, ConsistencyLevel.QUORUM)
    db_insert('movie_genres', movie_genres_sample, ConsistencyLevel.QUORUM)
    db_insert('movie_titles', movie_titles_sample, ConsistencyLevel.QUORUM)
    db_insert('movie_tags', movie_tags_sample, ConsistencyLevel.QUORUM)
    print('\tConsistency ALL:')
    db_insert('movie_ratings', movie_ratings_sample, ConsistencyLevel.ALL)
    db_insert('movie_details', movie_details_sample, ConsistencyLevel.ALL)
    db_insert('movie_genres', movie_genres_sample, ConsistencyLevel.ALL)
    db_insert('movie_titles', movie_titles_sample, ConsistencyLevel.ALL)
    db_insert('movie_tags', movie_tags_sample, ConsistencyLevel.ALL)


def db_query1(consistency):
    start = time.time()

    rows = []

    for i in range(0, 10):
        query = session.prepare("""
            SELECT movie_title, avg_rating
            FROM main.movie_ratings
            WHERE partition_key = 0
            ORDER BY avg_rating DESC
            LIMIT 30
        """)
        query.consistency_level = consistency
        rows = session.execute(query)

    end = time.time()
    print(f'\t\tQuery 1 (10 times) took {round(end - start, 4)} seconds')

    return pd.DataFrame(rows, columns=['movie_title', 'avg_rating'])


def db_query2(consistency):
    start = time.time()

    rows = []

    for i in range(0, 10):
        query = session.prepare("""
            SELECT movie_title, movie_genres, avg_rating, tag, tag_frequency
            FROM main.movie_details
            WHERE movie_title = 'Jumanji (1995)'
            ORDER BY tag_frequency DESC
            LIMIT 5
        """)
        query.consistency_level = consistency
        rows = session.execute(query)

    end = time.time()
    print(f'\t\tQuery 2 (10 times) took {round(end - start, 4)} seconds')

    return pd.DataFrame(rows, columns=['movie_title', 'movie_genres', 'avg_rating', 'tag', 'tag_frequency'])


def db_query3(consistency):
    start = time.time()

    rows = []

    for i in range(0, 10):
        query = session.prepare("""
            SELECT movie_title, movie_genre, movie_year
            FROM main.movie_genres
            WHERE movie_genre = 'Adventure'
            ORDER BY movie_year DESC
        """)
        query.consistency_level = consistency
        rows = session.execute(query)

    end = time.time()
    print(f'\t\tQuery 3 (10 times) took {round(end - start, 4)} seconds')

    return pd.DataFrame(rows, columns=['movie_title', 'movie_genre', 'movie_year'])


def db_query4(consistency):
    start = time.time()

    rows = []

    for i in range(0, 10):
        query = session.prepare("""
            SELECT movie_title
            FROM main.movie_titles
            WHERE movie_title_split CONTAINS 'star'
        """)
        query.consistency_level = consistency
        rows = session.execute(query)

    end = time.time()
    print(f'\t\tQuery 4 (10 times) took {round(end - start, 4)} seconds')

    return pd.DataFrame(rows, columns=['movie_title'])


def db_query5(consistency):
    start = time.time()

    rows = []

    for i in range(0, 10):
        query = session.prepare("""
            SELECT movie_title, avg_rating, tag
            FROM main.movie_tags
            WHERE tag = 'comedy'
            ORDER BY avg_rating DESC
            LIMIT 20
        """)
        query.consistency_level = consistency
        rows = session.execute(query)

    end = time.time()
    print(f'\t\tQuery 5 (10 times) took {round(end - start, 4)} seconds')

    return pd.DataFrame(rows, columns=['movie_title', 'avg_rating', 'tag'])


def queries():
    print('Testing query times for different consistency levels:')
    print('\tConsistency ONE:')
    db_query1(ConsistencyLevel.ONE)
    db_query2(ConsistencyLevel.ONE)
    db_query3(ConsistencyLevel.ONE)
    db_query4(ConsistencyLevel.ONE)
    db_query5(ConsistencyLevel.ONE)
    print('\tConsistency QUORUM:')
    db_query1(ConsistencyLevel.QUORUM)
    db_query2(ConsistencyLevel.QUORUM)
    db_query3(ConsistencyLevel.QUORUM)
    db_query4(ConsistencyLevel.QUORUM)
    db_query5(ConsistencyLevel.QUORUM)
    print('\tConsistency ALL:')
    res1 = db_query1(ConsistencyLevel.ALL)
    res2 = db_query2(ConsistencyLevel.ALL)
    res3 = db_query3(ConsistencyLevel.ALL)
    res4 = db_query4(ConsistencyLevel.ALL)
    res5 = db_query5(ConsistencyLevel.ALL)

    print('Query 1 results: ')
    print(res1.to_string())
    print('Query 2 results: ')
    print(res2.to_string())
    print('Query 3 results: ')
    print(res3.to_string())
    print('Query 4 results: ')
    print(res4.to_string())
    print('Query 5 results: ')
    print(res5.to_string())


if __name__ == '__main__':
    session = db_connect()
    db_setup()

    # create_and_insert_data()
    queries()

    session.shutdown()
