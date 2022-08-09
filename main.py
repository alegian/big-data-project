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
    # start = time.time()
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
    # end = time.time()
    # print(end - start)


def insert_movie_ratings(session):
    rating_insert_query = session.prepare("""
        INSERT INTO main.movie_ratings (user_id, movie_id, rating, rating_timestamp) 
        VALUES (?, ?, ?, ?)
    """)
    # session.execute(rating_insert_query, [int(row[0]), int(row[1]), float(row[2]), datetime.datetime.strptime(row[3], '%Y-%m-%d %H:%M:%S')])


def create_movie_ratings_csv():
    if not exists('movie_ratings.csv'):
        print('creating movie_ratings.csv...')
        movie_df = pd.read_csv('movie.csv')
        rating_df = pd.read_csv('rating.csv')

        movie_ratings_df = rating_df\
            .join(movie_df[['movieId', 'title']].set_index('movieId'), on='movieId')

        movie_ratings_df \
            .rename(columns={
                'rating': 'avg_rating',
                'movieId': 'movie_id',
                'title': 'movie_title',
                'userId': 'user_id',
                'timestamp': 'rating_timestamp',
            }) \
            .to_csv('movie_ratings.csv', index=False)


def create_movie_details_csv():
    if not exists('movie_details.csv'):
        print('creating movie_details.csv...')
        movie_df = pd.read_csv('movie.csv')
        rating_df = pd.read_csv('rating.csv')
        tag_df = pd.read_csv('tag.csv')

        avg_ratings_df = rating_df[['movieId', 'rating']] \
            .groupby('movieId') \
            .mean()\
            .round({'rating': 2})

        movie_details_df = avg_ratings_df \
            .join(movie_df[['movieId', 'title', 'genres']].set_index('movieId'), on='movieId')
        movie_details_df = movie_details_df \
            .join(tag_df[['movieId', 'tag']].set_index('movieId'), on='movieId')

        movie_details_df\
            .rename(columns={
                'rating': 'avg_rating',
                'movieId': 'movie_id',
                'title': 'movie_title',
                'genres': 'movie_genres'
            })\
            .to_csv('movie_details.csv', index=False)


def create_movie_genres_csv():
    if not exists('movie_genres.csv'):
        print('creating movie_genres.csv...')
        movie_df = pd.read_csv('movie.csv')

        movie_genres_df = movie_df.copy()
        movie_genres_df['movie_year'] = movie_df['title'].str.slice(-5, -1)

        movie_genres_df \
            .rename(columns={
                'genres': 'movie_genres',
                'movieId': 'movie_id',
                'title': 'movie_title'
            }) \
            .to_csv('movie_genres.csv', index=False)


if __name__ == '__main__':
    session = db_connect()
    db_setup(session)

    create_movie_ratings_csv()
    create_movie_details_csv()
    create_movie_genres_csv()

    session.shutdown()
