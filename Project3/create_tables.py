import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


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
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """Run functions in order
    drop_tables -> create_tables
    if all operations are succefully done print
    information about process, next close connection.
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    print('config read')
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    print('connenction establish')
    cur = conn.cursor()
    print('drop tables')
    drop_tables(cur, conn)
    print('create tables')
    create_tables(cur, conn)
    print('Tables are ready to use in {0}'.format(conn.dsn))
    conn.close()


if __name__ == "__main__":
    main()