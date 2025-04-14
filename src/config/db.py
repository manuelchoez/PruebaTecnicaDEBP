import sqlite3
conn = sqlite3.connect('test.db')

DB_PATH = 'src/config/test.db'

def get_connection():
    conn = sqlite3.connect(DB_PATH)
    return conn
