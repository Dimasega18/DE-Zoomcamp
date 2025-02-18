import pandas as pd
import argparse

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

        self.args = parser.parse_args()
        self.path = self.args.path
        self.table_name = self.args.table
        self.engine_url = f"postgresql://{self.args.user}:{self.args.password}@{self.args.host}:{self.args.port}/{self.args.db}"

    def extract(self) -> pd.DataFrame:
        """
        Extracts data from a CSV file and loads it into a Pandas DataFrame.

        Returns:
        pd.DataFrame: A DataFrame containing the extracted data.

        Raises:
        ValueError: If the file is not in CSV format.
        """

    def load(self, chunk_size: int, index: bool = False) -> str:
        """
        Loads the processed DataFrame into a PostgreSQL database.

        Parameters:
        table_name (str): Name of the table in the database.
        chunk_size (int): Number of rows to insert at a time.
        index (bool): Whether to include the DataFrame index.

        Returns:
        str: Confirmation message upon successful data insertion.
        """
        
        try :
            engine = create_engine(self.engine_url)

        except Exception as e :
            print('ERROR :',e)

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
