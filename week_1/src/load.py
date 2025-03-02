import pandas as pd
from sqlalchemy import text
import io
import time

def create_table(df: pd.DataFrame, table_name: str, session):
    """
    Creates a table in the database if it does not already exist.

    Parameters:
    df (pd.DataFrame): The DataFrame used to define the table schema.
    table_name (str): The name of the table to be created.
    session: SQLAlchemy session used to execute the query.
    """
    
    try:
        create_table_query = pd.io.sql.get_schema(df, name=table_name).replace('"', '').lower()
        create_table_query = create_table_query.replace("create table", "create table if not exists")

        session.execute(text(create_table_query))
        session.commit()

        print(f"Table '{table_name}' created successfully!")

    except Exception as e:
        print(f"Error creating table '{table_name}': {e}")


row_total = 0

def copy_insert(table, con, keys, data_iter) :

    time_start = time.time()

    data = list(data_iter)
    buffer = io.StringIO()
    columns = ','.join(keys).lower()
    table_name = table.name

    for row in data :
        buffer.write(','.join(map(str,row)) + '\n')

    buffer.seek(0)

    count_data = len(data)

    global row_total 
    row_total += count_data

    try:

        with con.connection.cursor() as cursor:
            cursor.copy_expert(
                f"COPY {table_name} ({columns}) FROM STDIN WITH (FORMAT CSV, DELIMITER ',')",
                buffer
            )

        print(f'{count_data} Data Inserted!, total row = {row_total}, time = {time.time() - time_start}')
    
    except Exception as e:
        con.rollback()
        print("Error during COPY operation, rolling back...")
        print(f"Error: {e}")
    
    finally:
        buffer.close()