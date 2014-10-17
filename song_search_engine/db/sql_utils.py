__author__ = 'Hari'

import sqlite3


def get_sql_connection(db_path):
    conn = sqlite3.connect(db_path)
    return conn.cursor()
