import psycopg2
import os
# Параметры подключения к базе данных
DB_NAME = os.environ.get('PG_NAME')
DB_USER = os.environ.get('PG_USER')
DB_PASSWORD = os.environ.get('PG_PASSWORD')
DB_HOST = os.environ.get('PG_HOST')
DB_PORT = '5432'


def connect_db():
    # подключение к базе данных
    try:
        conn = psycopg2.connect(database=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT)
        return conn
    except ConnectionError:
        print('Can not connect to Postgres')