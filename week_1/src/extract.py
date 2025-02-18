import pandas as pd

def read_csv_or_parquet(path: str) -> pd.DataFrame :
    """
    Parameters:
    path (str): The file path of the data to be loaded. This can either be a CSV file
                (with .csv extension) or a Parquet file (with .parquet extension).
    
    Returns:
    pd.DataFrame: A Pandas DataFrame containing the data from the CSV or Parquet file.
    """
    
    if path.endswith('.csv'):
        df = pd.read_csv(path, low_memory=False)
    elif path.endswith('.parquet'): #Recomended for large data
        df = pd.read_parquet(path)
    else :
        raise TypeError("Data must be in CSV or parquet format.")
    
    return df