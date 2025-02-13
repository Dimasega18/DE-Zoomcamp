from src import *

class etl_pipeline():
    def __init__(self, path: str):
        if path.endswith('.csv') :
            self.path = path
        else :
            raise TypeError('data must type CSV format.')

    def extract(self):
        df = pd.read_csv(self.path)
        return df

    def transform(self, df: pd.DataFrame):
        df = df.dropna()        
        df_cleaned = df.drop_duplicates()
        return df_cleaned

    def load(self, df: pd.DataFrame, chunk_size: int, table_name: str, database_url: str):
        connection = create_engine(database_url)
        df.to_sql(table_name,con=connection,if_exists='append',chunksize=chunk_size, index=False)
        
        