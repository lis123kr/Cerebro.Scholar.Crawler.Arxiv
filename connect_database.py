import os
import pymysql


def connect_database():
    conn = pymysql.connect(host=os.environ['DATABASE_HOST'],
                           port=int(os.environ['DATABASE_PORT']),
                           user=os.environ['DATABASE_USER'],
                           password=os.environ['DATABASE_PASSWORD'],
                           db=os.environ['DATABASE_NAME'],
                           charset='utf8mb4',
                           cursorclass=pymysql.cursors.DictCursor)
    return conn
