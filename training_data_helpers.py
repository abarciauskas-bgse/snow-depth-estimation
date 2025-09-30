import boto3
import pyarrow as pa
import pandas as pd
import pyarrow.parquet as pq
from typing import Dict, Any

import data_classes

training_data_fname = 'training_data.parquet'
bucket_name = 'aimees-snow-project'

def load_training_data() -> pd.DataFrame:
    s3 = boto3.client('s3')
    object_key = training_data_fname  # The key of the object in S3
    local_file_path = training_data_fname
    
    try:
        s3.download_file(bucket_name, object_key, local_file_path)
        print(f"File '{object_key}' downloaded successfully to '{local_file_path}'")
    except Exception as e:
        print(f"Error downloading file: {e}")

    return pd.read_parquet(local_file_path)
    

def prepare_training_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Data pre-processing
    * Add a month column
    * Set all fractional-snow covered area data to between 0 and 1000 as that is the valid range for this data (see [Landsat Collection 2 Level-3 Fractional Snow Covered Area Science Product ](https://www.usgs.gov/landsat-missions/landsat-collection-2-level-3-fractional-snow-covered-area-science-product)).
    """
    df['datetime'] = pd.to_datetime(df['date'])
    df['month'] = df['datetime'].dt.month
    df.loc[~df['fsca'].between(0, 1000), 'fsca'] = None
    return df

schema = pa.schema([
    pa.field('date', pa.string()),
    pa.field('snow_depth', pa.int64(), nullable=True),
    pa.field('red', pa.int64(), nullable=True),
    pa.field('green', pa.int64(), nullable=True),
    pa.field('blue', pa.int64(), nullable=True),
    pa.field('coastal', pa.float64(), nullable=True),
    pa.field('nir08', pa.int64(), nullable=True),
    pa.field('swir16', pa.int64(), nullable=True),
    pa.field('swir22', pa.int64(), nullable=True),
    pa.field('fsca', pa.float64(), nullable=True),
    pa.field('item_id', pa.string()),
    pa.field('station_triplet', pa.string()), 
    pa.field('latitude', pa.float64()),
    pa.field('longitude', pa.float64()),
    pa.field('elevation', pa.float64())
])

# Function for appending new data to parquet file
def append_training_rows(file_path, new_data):
    new_table = pa.table(new_data, schema=schema)
    
    # Read existing data and append
    existing_table = pq.read_table(file_path)
    combined_table = pa.concat_tables([existing_table, new_table])
    
    # Write back to file
    pq.write_table(combined_table, file_path)

def process_item_parallel(args: tuple) -> Dict[str, Any]:
    """Process a single item in parallel"""
    fs, item, lat, lon, station_triplet = args
    extractor = data_classes.HLSDataExtractor(fs=fs, item=item)
    ground_truth = data_classes.SNOTELProvider(
        station_triplet=station_triplet,
        latitude=lat,
        longitude=lon
    )
    manager = data_classes.SatelliteDataManager(
        extractor=extractor,
        ground_truth_provider=ground_truth
    )    
    training_points = manager.extract_training_data([(lat, lon)])
    return manager.filter_valid_training_data(training_points)
