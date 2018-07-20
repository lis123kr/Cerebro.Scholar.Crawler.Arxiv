import json
from connect_database import connect_database


def generate_json():
    conn = connect_database()
    with conn.cursor() as cursor:
        cursor.execute('SELECT * FROM articles')
        result = cursor.fetchall()
    with open('data.json', 'w') as outfile:
            json.dump(result, outfile)
    conn.close()
