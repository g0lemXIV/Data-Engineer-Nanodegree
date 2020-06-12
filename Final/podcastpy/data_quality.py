# coding=utf-8
"""Data quality checks"""
import logging
from config import config
import psycopg2

def count_list(list: list, name: str) -> None:
    """Counts data in list (useful for checking authors and feed lists).
    If list is empty raise error

    Parameters
    ----------
    list : list
        list to check
    name : str
        Name to print

    Returns
    -------
    None
    """
    count = len(list)
    if count < 1:
        raise ValueError(f"Data quality check failed. {name} list is empty ({count})")
    logging.info(f"Data quality on list {name} check passed with {len(name)} records")


def count_data(cur: psyconpg2.connect, table: str) -> None:
    """Data quality check which counting data in table.
    If table count is 0, raise error.
    Parameters
    ----------
    cur : psyconpg2.connect
        cursor of connection
    table : str
        table name to check
    """
    query = f"""SELECT count(*) FROM {table};"""
    cur.execute(query)
    results = cur.fetchone()
    if len(results) < 1 or results[0] < 1:
        raise ValueError(f"Data quality check failed. {table} returned no results")
    else:
    logging.info(f"Data quality on table {table} check passed with {results[0]} records")

def check_db(cur):
    """Check is connection is properly establish with database.
    If cur databse is different than dwg.cfg CLUSTER section, raise error.

    Parameters
    ----------
    cur : type
        cursor of connection
    """
    connection_dict = {k: eval(d) for k, d in dict(config['CLUSTER']).items()}
    # check if connect to proper Database
    q = f"""select current_database();"""
    cur.execute(q)
    results = cur.fetchone()[0]
    if connection_dict['db_name'] != results:
        raise ValueError(f"Data quality check failed. Wrong database ({results})")
