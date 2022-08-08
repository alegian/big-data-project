from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import csv
import datetime


def db_connect():
    cloud_config = {
        'secure_connect_bundle': './secure-connect-bigdataproject.zip'
    }
    auth_provider = PlainTextAuthProvider('oEkZhyIWrgyRFXeZRJtOZOZl',
                                          'gFFufu7mhm5reMhxNw,Si6KBNnAngSr9uPykcrZFZvSRALgZcU2Q6Oz.WLhAgsfGtFDsaP7rbFC9tQElawrNEW+FZ+inUFUOW,tPHPfpWk6pXvknmLG2Pnk6lWWTm4fu')
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


def insert_movie_ratings(session):
    rating_insert_query = session.prepare("""
        INSERT INTO main.movie_ratings (user_id, movie_id, rating, rating_timestamp) 
        VALUES (?, ?, ?, ?)
    """)
    with open('./rating.csv', newline='') as csvfile:
        reader = csv.reader(csvfile, delimiter=',')
        reader.__next__()
        for row in reader:
            session.execute(rating_insert_query, [int(row[0]), int(row[1]), float(row[2]), datetime.datetime.strptime(row[3], '%Y-%m-%d %H:%M:%S')])


if __name__ == '__main__':
    session = db_connect()
    db_setup(session)

    insert_movie_ratings(session)

    session.shutdown()