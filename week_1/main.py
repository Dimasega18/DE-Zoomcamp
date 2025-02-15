from src.data_utils import ETLPipeline

pipeline = ETLPipeline()

extract = pipeline.extract()

transform = extract.drop_duplicates()
transform.fill_null(cols = ['VendorID','RatecodeID'], method = 'custom', custom_value = 'Unknown', inplace = True)
transform.fill_null(cols = ['payment_type','store_and_fwd_flag'], method = 'mode', inplace = True)
transform.fill_null(cols = ['passenger_count'], method = 'median', inplace = True)
transform.detect_outliers(cols = ['passenger_count','trip_distance','fare_amount','extra','mta_tax','tip_amount','total_amount','congestion_surcharge'])

load = transform.load(chunk_size = 100_000, table_name = 'Yellow_tripdata_2021')
print(load)