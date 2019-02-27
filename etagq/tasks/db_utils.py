from celeryconfig import DB_USERNAME, DB_PASSWORD, DB_NAME, DB_HOST, DB_PORT
import celeryconfig

import logging

import sqlalchemy
from sqlalchemy.engine.url import URL
from sqlalchemy import create_engine, text

PG_DB = {
    'drivername': 'postgres',
    'username': DB_USERNAME,
    'password': DB_PASSWORD,
    'database': DB_NAME,
    'host': DB_HOST,
    'port': DB_PORT
    }

ENGINE = create_engine(URL(**PG_DB))


def _connect_db():
    try:
        conn = ENGINE.connect()
        return conn
    except sqlalchemy.exc.OperationalError as e:
        logging.error("Error with DB connection:\n{0}".format(e))
        return None


def get_columns(table, columns):
    """ return cursor from table with defined columns """
    query = "select :columns from :table"
    conn = _connect_db()
    #if conn:
    #    return conn.execute(text(query), table=table, columns=columns).fetchall()
    #return None
    return {"testing": "reached get columns"}

