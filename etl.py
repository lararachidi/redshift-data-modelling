import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Copy data from JSON files contained in S3 buckets to the Redshift staging tables.
    cur, conn: cursor and connection objects to connect to the database.
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Insert data from the staging tables into the fact and dimension tables.
    cur, conn: cursor and connection objects to connect to the database.
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Connect to the Redshift cluster specified in the config file. 
    Call functions to load S3 data into the Redshift staging tables.
    Insert relevant data into the fact and dimension tables. 
    Close the connection to the cluster upon completion.
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()