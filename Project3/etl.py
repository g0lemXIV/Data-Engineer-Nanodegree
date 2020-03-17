import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries
import time


def load_staging_tables(cur, conn):
    """Function loading staging tables, it
    use COPY method from AWS Redshift to copy
    file from s3 to staging files.
    Parameters
    ----------
    cur: psyconpg2.connect
        cursor of connection
    conn: psyconpg2.connect.cursor
        connecition function
    """
    for query in copy_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """Function insert data into star
    schema tables.
    Parameters
    ----------
    cur: psyconpg2.connect
        cursor of connection
    conn: psyconpg2.connect.cursor
        connecition function
    """
    for query in insert_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()


def main():
    """Connect to AWS database,
    run all process function and close connection.
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    # measure time
    start = time.time()
    load_staging_tables(cur, conn)
    end = time.time()
    print('staging time', end - start)
    start = time.time()
    insert_tables(cur, conn)
    end = time.time()
    print('insert time', end - start)
    conn.close()


if __name__ == "__main__":
    main()