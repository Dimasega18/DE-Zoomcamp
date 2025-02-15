from src import *

class ETLPipeline:
    """
    ETLPipeline is a tool to extract, transform, and load (ETL) CSV data into a PostgreSQL database.
    """

    def __init__(self):
        """
        Initializes the ETL pipeline with command-line arguments for database connection and CSV file URL.
        """
        parser = argparse.ArgumentParser(
            prog='csv_to_pg',
            description='A tool to efficiently download a CSV file and insert its data into a PostgreSQL database.',
            epilog='Ensure that the database connection parameters are correctly set before running.'
        )

        parser.add_argument('-u', '--user', required=True, help="PostgreSQL username")
        parser.add_argument('-P', '--password', required=True, help="PostgreSQL password")
        parser.add_argument('-p', '--port', default=5432, type=int, help="PostgreSQL port (default: 5432)")
        parser.add_argument('-d', '--db', required=True, help="PostgreSQL database name")
        parser.add_argument('-H', '--host', default='localhost', help="PostgreSQL host (default: localhost)")
        parser.add_argument('--path', required=True, help="path of the CSV file to download")

        self.args = parser.parse_args()
        self.path = self.args.path
        self.engine_url = f"postgresql://{self.args.user}:{self.args.password}@{self.args.host}:{self.args.port}/{self.args.db}"

    def extract(self) -> pd.DataFrame:
        """
        Extracts data from a CSV file.

        Parameters:
        path (str): The file path to the CSV.

        Returns:
        pandas.DataFrame: A DataFrame containing the extracted data.

        Raises:
        ValueError: If the file is not in CSV format.
        """
        if not self.path.endswith('.csv'):
            raise ValueError("Data must be in CSV format.")
        
        df = pd.read_csv(self.path,low_memory=False)

        self.df = df

        return self.df

    def fill_null(self, cols: list, method: str = 'mode', inplace: bool = False, custom_value=None) -> pd.DataFrame:
        """
        Fills null values in specified columns using the chosen method (mode, median, mean, or a custom value).

        Parameters:
        df (pd.DataFrame): The DataFrame where null values should be filled.
        cols (list): List of column names to process.
        method (str): The filling method ('mode', 'median', 'mean', or 'custom'). Default is 'mode'.
        inplace (bool): If True, modifies the DataFrame in place. If False, returns a new DataFrame.
        custom_value (any, optional): The value to use when method='custom'.

        Returns:
        pd.DataFrame: The DataFrame with filled null values (if inplace=False).
        """
        if cols is None:
            raise ValueError("You must specify at least one column in 'cols'.")

        if not all(col in self.df.columns for col in cols):
            raise ValueError("One or more specified columns are not present in the DataFrame.")

        if method not in ['mode', 'median', 'mean', 'custom']:
            raise ValueError("The 'method' parameter must be 'mode', 'median', 'mean', or 'custom'.")

        if method == 'custom' and custom_value is None:
            raise ValueError("If method='custom', you must provide a 'custom_value'.")

        if inplace:
            for col in cols:
                if self.df[col].isnull().sum() > 0: 
                    if method == 'mode':
                        self.df[col].fillna(self.df[col].mode()[0], inplace=True)
                    elif method == 'median':
                        self.df[col].fillna(self.df[col].median(), inplace=True)
                    elif method == 'mean':
                        self.df[col].fillna(self.df[col].mean(), inplace=True)
                    else:
                        self.df[col].fillna(custom_value, inplace=True)
        else:
            self.df = self.df.copy()
            for col in cols:
                if self.df[col].isnull().sum() > 0:
                    if method == 'mode':
                        self.df[col] = self.df[col].fillna(self.df[col].mode()[0])
                    elif method == 'median':
                        self.df[col] = self.df[col].fillna(self.df[col].median())
                    elif method == 'mean':
                        self.df[col].fillna(self.df[col].mean())
                    else:
                        self.df[col] = self.df[col].fillna(custom_value)

            return self.df
    
    def detect_outliers(self, cols: list = None, threshold: float = 3) -> pd.DataFrame:
        """
        Detects and removes rows with outliers based on Z-score method.

        Parameters:
        df (pd.DataFrame): The input DataFrame.
        cols (list, optional): List of column names to check for outliers. If None, all numeric columns will be used.
        threshold (float): The Z-score threshold beyond which a value is considered an outlier (default: 3).

        Returns:
        pd.DataFrame: A DataFrame with outliers removed.
        """
        cols_number= self.df.select_dtypes(include=['number']).columns.tolist()

        if cols is None:
            cols = cols_number

        if not all(col in cols_number for col in cols):
            raise ValueError("One or more specified columns are not valid numeric columns.")

        numeric_df = self.df.loc[:,cols].copy()

        mean_vals = numeric_df.mean()
        std_vals = numeric_df.std()

        std_vals[std_vals == 0] = 1  

        z_scores = (numeric_df - mean_vals) / std_vals
        outlier_mask = (abs(z_scores) > threshold).any(axis=1)

        self.df = self.df[~outlier_mask]

        return self.df

    def load(self, chunk_size: int, table_name: str, index: bool = False):
        """
        Loads the cleaned data into a PostgreSQL database with progress tracking.

        Parameters:
        chunk_size (int): The number of rows to insert at a time.
        table_name (str): The name of the database table.
        index (bool): Whether to include the DataFrame index.

        Returns:
        str: Confirmation message with total rows inserted and time taken.
        """

        connection = create_engine(self.engine_url)

        start_time = time()
        total_rows = len(self.df)

        self.df.to_sql(table_name, con=connection, if_exists='append', chunksize=chunk_size, index=index)

        end_time = time()

        return f"Data insertion complete!, total row = {total_rows}, time = {end_time - start_time}"

    def __repr__(self):
        """
        Official string representation of the ETLPipeline instance.

        Returns:
        str: A string with information about the pipeline's database connection.
        """
        return f"ETLPipeline(user='{self.args.user}', db='{self.args.db}', host='{self.args.host}', port={self.args.port})"

    def __str__(self):
        """
        User-friendly string representation of the ETLPipeline instance.

        Returns:
        str: A readable string describing the database connection.
        """
        return f"ETL Pipeline connected to PostgreSQL at {self.args.host}:{self.args.port}, database: {self.args.db}"

if __name__ == '__main__':
    pipeline = ETLPipeline()
    print(pipeline.__repr__())
    print(pipeline)
