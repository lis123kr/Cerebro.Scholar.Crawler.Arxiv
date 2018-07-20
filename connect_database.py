import os
import pymysql

conn = pymysql.connect(host=os.environ['DATABASE_HOST'],
                       port=int(os.environ['DATABASE_PORT']),
                       user=os.environ['DATABASE_USER'],
                       password=os.environ['DATABASE_PASSWORD'],
                       db='cerebro_scholar_crawler_arxiv',
                       charset='utf8mb4',
                       cursorclass=pymysql.cursors.DictCursor)
