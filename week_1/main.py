from week_1.src import *
import pandas as pd

def type_writer(text, delay=0.05):
        """
        Simulates a typing effect by printing characters one by one.

        Parameters:
        text (str): Text to display.
        delay (float): Delay between each character.
        """
        for char in text:
            sys.stdout.write(char)
            sys.stdout.flush()
            sleep(delay)
        print()


pipeline = ETL()

# Extract
ETL.type_writer(text='🔍 Extracting Data ...',delay = 0.2)
extract = pipeline.extract()
ETL.type_writer(text='✅ Extraction complete!',delay = 0)

# Transform
ETL.type_writer(text='🔄 Transforming data...',delay = 0.2)
transform = extract.drop_duplicates()
transform = pipeline.fill_null(cols = ['VendorID','RatecodeID'], method = 'custom', custom_value = 'Unknown')
transform = pipeline.fill_null(cols = ['payment_type','store_and_fwd_flag'], method = 'mode')
transform = pipeline.fill_null(cols = ['passenger_count'], method = 'median')
transform = pipeline.detect_outliers(cols = [
    'passenger_count',
    'trip_distance',
    'fare_amount',
    'extra',
    'mta_tax',
    'tip_amount',
    'total_amount',
    'congestion_surcharge'
    ])
ETL.type_writer(text='✅ Transforming complete!', delay = 0)

# Load
ETL.type_writer(text='📤 Loading data...', delay = 0.5)
load = pipeline.load(chunk_size = 10_000, table_name = 'Yellow_tripdata_2021')
ETL.type_writer(text='✅ Loading complete!',delay = 0)