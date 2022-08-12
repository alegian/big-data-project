from datetime import datetime

from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from os.path import exists
import os
import pandas as pd
import time
from dotenv import load_dotenv
load_dotenv()


def db_connect():
    cloud_config = {
        'secure_connect_bundle': './secure-connect-bigdataproject.zip'
    }
    auth_provider = PlainTextAuthProvider(os.environ.get('DB_USER'), os.environ.get('DB_SECRET'))
    cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
    session = cluster.connect('main')
    # session.default_consistency_level = ConsistencyLevel.ONE
    return session


def db_setup(session):
    session.execute("""
    CREATE TABLE IF NOT EXISTS main.movie_ratings (
        movie_title text, 
        movie_id int, 
        user_id int, 
        rating float,
        rating_timestamp timestamp,
        PRIMARY KEY (movie_id, rating, user_id)
    ) WITH CLUSTERING ORDER BY (rating DESC, user_id ASC);
    """)
    session.execute("""
        CREATE TABLE IF NOT EXISTS main.movie_details (
            movie_title text, 
            movie_id int, 
            movie_genres text, 
            avg_rating float,
            tag text,
            PRIMARY KEY ((movie_id, tag), avg_rating)
        ) WITH CLUSTERING ORDER BY (avg_rating DESC);
        """)
    session.execute("""
        CREATE TABLE IF NOT EXISTS main.movie_genres (
            movie_title text, 
            movie_id int, 
            movie_genres text, 
            movie_year text,
            PRIMARY KEY (movie_id)
        );
        """)


def insert_db(session, table_name, df):
    start = time.time()

    for row in df.values.tolist():
        # variable table name, variable number of columns
        insert_query = session.prepare(f"""
            INSERT INTO main.{table_name} ({','.join(df.columns.values.tolist())}) 
            VALUES ({','.join(['?']*len(df.columns))})
        """)
        session.execute(insert_query, row)

    end = time.time()
    print(f'Insert 1000 rows into {table_name} took {round(end-start, 4)} seconds')


def create_movie_ratings():
    # load data
    movie_df = pd.read_csv('movie.csv')
    rating_df = pd.read_csv('rating.csv')
    # generate table data
    movie_ratings_df = rating_df\
        .join(movie_df[['movieId', 'title']].set_index('movieId'), on='movieId')

    # rename for compatibility
    movie_ratings_df = movie_ratings_df.reset_index() \
        .rename(columns={
            'movieId': 'movie_id',
            'title': 'movie_title',
            'userId': 'user_id',
            'timestamp': 'rating_timestamp',
        })

    # return the first 1000 rows, and create a CSV with the rest
    sample = movie_ratings_df.head(1000)
    if not exists('movie_ratings.csv'):
        movie_ratings_df = movie_ratings_df.tail(len(movie_ratings_df.index) - 1000)
        print('creating movie_ratings.csv...')
        movie_ratings_df.to_csv('movie_ratings.csv', index=False)
    return sample


def create_movie_details():
    # load data
    movie_df = pd.read_csv('movie.csv')
    rating_df = pd.read_csv('rating.csv')
    tag_df = pd.read_csv('tag.csv')

    # generate table data
    avg_ratings_df = rating_df[['movieId', 'rating']] \
        .groupby('movieId') \
        .mean()\
        .round({'rating': 2})
    movie_details_df = avg_ratings_df \
        .join(movie_df[['movieId', 'title', 'genres']].set_index('movieId'), on='movieId')
    movie_details_df = movie_details_df \
        .join(tag_df[['movieId', 'tag']].set_index('movieId'), on='movieId')

    # rename for compatibility
    movie_details_df = movie_details_df.reset_index() \
        .rename(columns={
            'rating': 'avg_rating',
            'movieId': 'movie_id',
            'title': 'movie_title',
            'genres': 'movie_genres'
        })

    # return the first 1000 rows, and create a CSV with the rest
    sample = movie_details_df.head(1000)
    if not exists('movie_details.csv'):
        movie_details_df = movie_details_df.tail(len(movie_details_df.index) - 1000)
        print('creating movie_details.csv...')
        movie_details_df.to_csv('movie_details.csv', index=False)
    return sample


def create_movie_genres():
    # load data
    movie_df = pd.read_csv('movie.csv')

    # generate table data
    movie_genres_df = movie_df.copy()
    movie_genres_df['movie_year'] = movie_df['title'].str.slice(-5, -1)

    # rename for compatibility
    movie_genres_df = movie_genres_df.reset_index() \
        .rename(columns={
            'genres': 'movie_genres',
            'movieId': 'movie_id',
            'title': 'movie_title'
         })

    # return the first 1000 rows, and create a CSV with the rest
    sample = movie_genres_df.head(1000)
    if not exists('movie_genres.csv'):
        movie_genres_df = movie_genres_df.tail(len(movie_genres_df.index) - 1000)
        print('creating movie_genres.csv...')
        movie_genres_df.to_csv('movie_genres.csv', index=False)
    return sample


if __name__ == '__main__':
    session = db_connect()
    db_setup(session)

    # movie_ratings_sample = create_movie_ratings()
    movie_details_sample = create_movie_details()
    movie_genres_sample = create_movie_genres()

    # fix dates
    # movie_ratings_sample['rating_timestamp'] = movie_ratings_sample['rating_timestamp'] \
    #     .apply(lambda d: datetime.strptime(d, '%Y-%m-%d %H:%M:%S'))

    # insert_db(session, 'movie_ratings', movie_ratings_sample)
    insert_db(session, 'movie_details', movie_details_sample)
    insert_db(session, 'movie_genres', movie_genres_sample)

    session.shutdown()
