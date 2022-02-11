import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """Drop tables from the database

    Executes queries to drop all tables from database.
    
    Args:
        cur: cursor to the database's connection. Type: psycopg2.cursor
        conn: connection to the database. Type: psycopg2.connect
    
    Raises:
        psycopg2.Error: if there is an error with the database connection
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """Create tables in the database
    
    Executes queries to create all tables in database.

    Args:
        cur: cursor to the database's connection. Type: psycopg2.cursor
        conn: connection to the database. Type: psycopg2.connect

    Raises:
        psycopg2.Error: if there is an error with the database connection
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """Main function to drop and create tables in the database
    
    Steps:
        1. Read the dwh.cfg file to get the connection parameters.
        2. Connect to the database.
        3. Call functions to drop tables.
        4. Call functions to create tables.
        5. Close the database connection.

    Raise:
        psycopg2.Error: if there is an error with the database connection
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(
        *config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
