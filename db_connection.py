import psycopg2
from config import config

def get_db_connection():
    host = config['host']
    user = config['user']
    pwd = config['pwd']
    port = config['port']
    dbname = config['dbname']
    conn = psycopg2.connect(f"dbname={dbname} user={user} host={host} password={pwd} port={port}")
    conn.set_session(autocommit=True)
    return conn.cursor()