from src.ETLPipeline import ETLPipeline as etl
from src.data_utils import type_writer

pipeline = etl()

# Extract
type_writer('📁 Extract...',0.5)
pipeline.extract()

#Transform
type_writer('🔃 Transform...')
pipeline.transform(
    fill_null_operations=[
        {'cols': ['VendorID', 'RatecodeID'], 'method': 'custom', 'custom_value': 'Unknown'},
        {'cols': ['payment_type', 'store_and_fwd_flag'], 'method': 'mode'},
        {'cols': ['passenger_count'], 'method': 'median'}
    ],
    cols_remove_outliers=[
        'passenger_count', 'trip_distance', 'fare_amount', 'extra',
        'mta_tax', 'tip_amount', 'total_amount', 'congestion_surcharge'
    ],
    threshold=3
)

# Load
type_writer('📃 Load...',0.5)
pipeline.load()