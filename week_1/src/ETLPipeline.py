import pandas as pd
import argparse
from src import extract,transform,load
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine

class ETLPipeline:
    """
    ETLPipeline is a tool for extracting, transforming, and loading (ETL) CSV data into a PostgreSQL database.
    """

    def __init__(self):
        """
        Initializes the ETL pipeline with command-line arguments for database connection and CSV file path.
        """
        parser = argparse.ArgumentParser(
            prog='csv_to_pg',
            description='A tool to efficiently load CSV data into a PostgreSQL database.',
            epilog='Ensure the database connection parameters are correctly set before running.'
        )

        parser.add_argument('-u', '--user', required=True, help="PostgreSQL username")
        parser.add_argument('-P', '--password', required=True, help="PostgreSQL password")
        parser.add_argument('-p', '--port', default=5432, type=int, help="PostgreSQL port (default: 5432)")
        parser.add_argument('-d', '--db', required=True, help="PostgreSQL database name")
        parser.add_argument('-H', '--host', default='localhost', help="PostgreSQL host (default: localhost)")
        parser.add_argument('--path', required=True, help="Path of the CSV file")
        parser.add_argument('-tb','--table', required=True, help="Table name")
        parser.add_argument('-cz','--chunksize', required=True, type=int, help="Chunk size of data")

        self.args = parser.parse_args()
        self.path = self.args.path
        self.table_name = self.args.table
        self.chunksize = self.args.chunksize
        self.engine_url = f"postgresql://{self.args.user}:{self.args.password}@{self.args.host}:{self.args.port}/{self.args.db}"

    def extract(self) :
        self.df = extract.read_csv_or_parquet(self.path)

        return self.df

    def transform(self, fill_null_operations: list, cols_remove_outliers: list, threshold: int = 3):
        self.df = self.df.drop_duplicates()
    
        for operation in fill_null_operations:
            self.df = transform.fill_null(
                self.df, 
                operation['cols'], 
                operation['method'], 
                operation.get('custom_value')
            )
    
        self.df = transform.remove_outliers(self.df, threshold, cols_remove_outliers)
    
        return self.df
    
    def load(self, index: bool = False) :

        self.df.columns = self.df.columns.str.lower()
        engine_db = create_engine(self.engine_url)
        Session = sessionmaker(bind = engine_db)
        session = Session()

        try :
                load.create_table(self.df, self.table_name, session)
                self.df.to_sql(self.table_name, con=session.bind ,chunksize=self.chunksize, index=False, if_exists='append', method=load.copy_insert)
                print('Data successfully inserted!')

        except Exception as e:
            session.rollback()
            print('Table has been rollback!')
            print('Error:', e)

        finally :
            session.close()

        return self.df

    def __repr__(self):
        """
        Returns the official string representation of the ETLPipeline instance.
        """
        return f"ETLPipeline(user='{self.args.user}', db='{self.args.db}', host='{self.args.host}', port={self.args.port})"

    def __str__(self):
        """
        Returns a user-friendly string representation of the ETLPipeline instance.
        """
        return f"ETL Pipeline connected to PostgreSQL at {self.args.host}:{self.args.port}, database: {self.args.db}"

if __name__ == '__main__':
    pipeline = ETLPipeline()
    print(pipeline.__repr__())
    print(pipeline)
