import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """Load data from S3 to staging tables

    Steps:
        1. Execute queries to load data from S3 to staging tables including stg_events and stg_songs.
        2. Execute queries to load data from stg_events to stg_users and stg_songs to stg_artists

    Args:
        cur: cursor to the database's connection. Type: psycopg2.cursor
        conn: connection to the database. Type: psycopg2.connect
    
    Raises:
        psycopg2.Error: if there is an error with the database connection
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """Insert data from staging tables to the fact and dimension tables
    
    Steps:
        1. Execute queries to insert data into songs from stg_songs.
        2. Execute queries to insert data into artists from stg_artists.
        3. Execute queries to insert data into users from stg_users.
        4. Execute queries to insert data into time from stg_events.
        5. Execute queries to insert data into songplays from stg_events.
    
    Args:
        cur: cursor to the database's connection. Type: psycopg2.cursor
        conn: connection to the database. Type: psycopg2.connect

    Raises:
        psycopg2.Error: if there is an error with the database connection
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """Main function to load data from S3 to staging tables and insert data from staging tables to the fact and dimension tables
    
    Steps:
        1. Read the dwh.cfg file to get the connection parameters.
        2. Connect to the database.
        3. Call functions to load data to staging tables.
        4. Call functions to insert data from staging tables to the fact and dimension tables.
        5. Close the database connection.

    Raises:
        psycopg2.Error: if there is an error with the database connection
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(
        *config['CLUSTER'].values()))
    cur = conn.cursor()

    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
