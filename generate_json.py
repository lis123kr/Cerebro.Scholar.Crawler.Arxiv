import json
from connect_database import conn


def generate_json():
    with conn.cursor() as cursor:
        cursor.execute('SELECT * FROM articles')
        result = cursor.fetchall()
    with open('data.json', 'w') as outfile:
            json.dump(result, outfile)
