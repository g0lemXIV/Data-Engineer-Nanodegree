import psycopg2
import logging
from sql_queries import create_table_queries, drop_table_queries, enum_queries


def create_database():
    """
    The function creates sparkify database.
    If the database exists first it and creates
    a new database. For now, all connecting parameters
    are entered into psycopg2.connect but it is not a safe practice.
    
    Returns
    -------
    cur: psyconpg2.connect
        cursor of connection
    conn: psyconpg2.connect.cursor
        connecition function
    """
    # connect to default database
    conn = psycopg2.connect("host=127.0.0.1 dbname=studentdb user=student password=student")
    conn.set_session(autocommit=True)
    cur = conn.cursor()
    
    # create sparkify database with UTF8 encoding
    cur.execute("DROP DATABASE IF EXISTS sparkifydb")
    cur.execute("CREATE DATABASE sparkifydb WITH ENCODING 'utf8' TEMPLATE template0")
    print('Successfully create database {0}'.format(conn.dsn))
    # close connection to default database
    conn.close()    
    
    # connect to sparkify database
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()
    
    return cur, conn

def create_enum(cur, conn):
    """Create enumerated types base on enum_queries
    and commit results into the database immediately.
    Parameters
    ----------
    cur: psyconpg2.connect
        cursor of connection
    conn: psyconpg2.connect.cursor
        connecition function
    """
    for query in enum_queries:
        cur.execute(query)
        conn.commit()

def drop_tables(cur, conn):
    """Drop all tables base on drop_table_queries
    if exists and commit results into the database immediately.
    Parameters
    ----------
    cur: psyconpg2.connect
        cursor of connection
    conn: psyconpg2.connect.cursor
        connecition function
    """
    print('Drop tables from {0}'.format(conn.dsn))
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """Create all tables base on create_table_queries
    commit results into the database immediately.
    Parameters
    ----------
    cur: psyconpg2.connect
        cursor of connection
    conn: psyconpg2.connect.cursor
        connecition function
    """
    print('Create tables in {0}'.format(conn.dsn))
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()
        
def main():
    """Run functions in order
    drop_tables -> create_enum -> create_tables
    if all operations are succefully done print
    information about process, next close connection.
    """
    cur, conn = create_database()
    
    drop_tables(cur, conn)
    create_enum(cur, conn)
    create_tables(cur, conn)
    print('Tables are ready to use in {0}'.format(conn.dsn))

    conn.close()


if __name__ == "__main__":
    main()