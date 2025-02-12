from src import *

class etl_pipeline:

    def extract(url: str, output_path: str):
import os
import subprocess
import pandas as pd

def extract(url: str, output_path: str):
    """
    Mengunduh file CSV dari URL dan membacanya ke dalam Pandas DataFrame.
    Args:
    url (str): URL tempat file CSV diunduh.
    output_path (str): Nama file output yang akan disimpan.
    
    Returns:
    pd.DataFrame: Data dari file CSV yang diunduh.
    
    Raises:
    ValueError: Jika `output_path` bukan file CSV.
    RuntimeError: Jika `wget` gagal mengunduh file.
    pd.errors.EmptyDataError: Jika file CSV kosong atau tidak valid.
    Exception: Error lain saat membaca CSV.
    """

    if not output_path.lower().endswith('.csv') :
        raise ValueError("Output file must be a CSV file (e.g., 'data.csv').")
    
    try:
        subprocess.run(["wget", "-O", output_path, url], check=True)
        df = pd.read_csv(output_path)

    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"Failed to download file: {e}") from e
    
    except pd.errors.EmptyDataError:
        raise ValueError("The downloaded CSV file is empty or corrupt.")
    
    except Exception as e:
        raise RuntimeError(f"An error occurred while reading the CSV file: {e}") from e
    
    return df


        
    def transform(df: pd.DataFrame):

    def load():