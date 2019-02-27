from celeryconfig import DB_USERNAME, DB_PASSWORD, DB_NAME, DB_HOST, DB_PORT

import sqlalchemy
from sqlalchemy.engine.url import URL
from sqlalchemy import create_engine, text

import logging

pg_db = {
    'drivername': 'postgres',
    'username': DB_USERNAME,
    'password': DB_PASSWORD,
    'database': DB_NAME,
    'host': DB_HOST,
    'port': DB_PORT
    }

engine = create_engine(URL(**pg_db))


def _connect_db():
    try:
        conn = engine.connect()
        return conn
    except sqlalchemy.exc.OperationalError as e:
        logging.error("Error with DB connection:\n{0}".format(e))
        return None


def get_columns(table, columns):
    query = "select :columns from :table"
    conn = _connect_db()
    if conn:
        return conn.execute(text(query), table=table, columns=columns).fetchall() 
    else:
        return None

