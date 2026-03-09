import os
from contextlib import contextmanager

import psycopg2
from pymongo import MongoClient


def get_mongo_db():
    client = MongoClient(os.getenv('MONGO_URI', 'mongodb://mongo:27017'))
    return client[os.getenv('MONGO_DB', 'app_source')]


@contextmanager
def pg_conn(dbname: str | None = None):
    conn = psycopg2.connect(
        host=os.getenv('DWH_HOST', 'postgres'),
        port=int(os.getenv('DWH_PORT', '5432')),
        user=os.getenv('DWH_USER', 'etl_user'),
        password=os.getenv('DWH_PASSWORD', 'etl_password'),
        dbname=dbname or os.getenv('DWH_DB', 'dwh'),
    )
    try:
        yield conn
    finally:
        conn.close()