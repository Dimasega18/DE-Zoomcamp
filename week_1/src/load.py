import pandas as pd
from sqlalchemy import engine, text, create_engine
from sqlalchemy.orm import sessionmaker
import io
import time

def create_table(df: pd.DataFrame, table_name: str, session):
    """
    Creates a table in the database if it does not exist
    Parameters:
    df (pd.DataFrame): DataFrame to create the table from.
    table_name (str): Name of the table to create.
    engine: SQLAlchemy engine to execute the query.
    """
    try:
        create_table_query = pd.io.sql.get_schema(df, name=table_name).replace('"', '').lower()
        create_table_query = create_table_query.replace("create table", "create table if not exists")

        # Execute the create table query
        session.execute(text(create_table_query))
        session.commit()

        print(f"Table '{table_name}' created successfully!")

    except Exception as e:
        print(f"Error creating table '{table_name}': {e}")

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
            time.sleep(0.3)
        # Jika berhasil, commit otomatis karena pakai `with con.begin()`
        print(f'{count_data} Data Inserted!, total row = {row_total}, time = {time.time() - time_start}')
    
    except Exception as e:
        con.rollback()  # Rollback jika terjadi error
        print("Error during COPY operation, rolling back...")
        print(f"Error: {e}")
    
    finally:
        buffer.close()  # Pastikan buffer selalu tertutup



df = pd.read_csv(r'C:\Users\dimas\OneDrive\Desktop\DE-Zoomcamp\week_1\data\yellow_tripdata_2021-01.csv',low_memory=False).head(100)
df.columns = df.columns.str.lower()
row_total = 0

engine_db = create_engine("postgresql://admin:seadminituh@localhost:5433/public")
Session = sessionmaker(bind=engine_db)
session = Session()

try :
    create_table(df,'udin', session)
    df.to_sql('udin',con = engine_db ,chunksize=10, index=False, if_exists='append', method=copy_insert)
    print('Data successfully inserted!')

except Exception as e:
    session.rollback()
    print('Table has been rollback!')
    print('Error:', e)

finally :
    session.close()
