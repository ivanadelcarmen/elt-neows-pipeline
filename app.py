from configparser import ConfigParser
from datetime import timedelta
from math import log

# Import program-specific modules
from utils import *
from extract import get_data, build_table
from load import save_new_data, upsert_data
from transform import clean_table, inner_join
from optimize import z_order_table


def extract_and_load(tgt_path: str, api_url: str, incremental_values: dict, default_values: dict):
    # Add default parameters to incremental parameters
    incremental_values.update(default_values)

    # Extraction: close_approach_data
    records = get_data(api_url, 'feed', params=incremental_values, field='near_earth_objects')
    close_approach_data = build_table(records, 'close_approach_data')

    # Extract all the NEOs IDs as a list 
    id_list = close_approach_data['neo_reference_id'].tolist()

    # Extraction: asteroid_data
    asteroids = get_data(api_url, 'neo', params=default_values, lookup=id_list)
    asteroid_data = build_table(asteroids, 'asteroid_data')

    # Loading: asteroid_data
    upsert_data(asteroid_data, f'{tgt_path}/asteroid_data',
                predicate=f'tgt.neo_reference_id = src.neo_reference_id')

    # Loading: close_approach_data
    save_new_data(close_approach_data,
                  f'{tgt_path}/close_approach_data',
                  predicate=f'''tgt.approach_datetime = src.approach_datetime 
                    AND tgt.neo_reference_id = src.neo_reference_id''',
                  partition_cols=['year', 'month', 'week'])
    
    # Optimize the reading of close_approach_data for the next stage
    z_order_table(f'{tgt_path}/close_approach_data', ordering_columns=['extraction_date'])


def cleaning_transform(src_path: str, tgt_path: str, schemas_path: str):
    # Read both tables in Delta Lake format for cleaning
    asteroid_data = read_delta_table(f'{src_path}/asteroid_data')
    close_approach_data = read_delta_table(f'{src_path}/close_approach_data') # Reads the last batch extracted

    schemas = read_json(schemas_path) # Read the schemas file for the transformation

    # Cleaning: close_approach_data
    close_approach_data = clean_table(close_approach_data, schemas, 'close_approach_data')

    # Cleaning: asteroid_data
    asteroid_data = clean_table(asteroid_data, schemas, 'asteroid_data')

    # Loading: asteroid_data
    upsert_data(asteroid_data, f'{tgt_path}/asteroid_data',
                predicate=f'tgt.neo_reference_id = src.neo_reference_id')

    # Loading: close_approach_data
    save_new_data(close_approach_data,
                  f'{tgt_path}/close_approach_data',
                  predicate=f'''tgt.approach_datetime = src.approach_datetime 
                    AND tgt.neo_reference_id = src.neo_reference_id''',
                  partition_cols=['year', 'month', 'week'])
    
    # Optimize the reading of close_approach_data for the next stage
    z_order_table(f'{tgt_path}/close_approach_data', ordering_columns=['extraction_date'])


def analytic_transform(src_path: str, tgt_path: str, schemas_path: str):
    # Read both tables in Delta Lake format for analytic operations
    asteroid_data = read_delta_table(f'{src_path}/asteroid_data')
    close_approach_data = read_delta_table(f'{src_path}/close_approach_data') # Reads the last batch extracted

    # Add columns to asteroid_data with the average estimated diameter and albedo of each asteroid
    insertion_index = len(asteroid_data.columns) - 1

    average_diameter = asteroid_data.apply(
        lambda row: (row['estimated_diameter_min'] + row['estimated_diameter_max']) / 2, axis=1)
    asteroid_data.insert(insertion_index, 'estimated_diameter_avg', average_diameter)

    albedo = asteroid_data.apply(
        lambda row: 10 ** (-2 * log(row['estimated_diameter_avg'], 10) + 6.2472 - 0.4 * row['absolute_magnitude_h']), axis=1)
    asteroid_data.insert(insertion_index, 'albedo', albedo)

    # Separate in close_approach_data the 'approach_datetime' column into date and time.
    close_approach_data['approach_date'] = close_approach_data['approach_datetime'].dt.date \
                                            .astype('datetime64[ns]')
    close_approach_data['approach_time'] = close_approach_data['approach_datetime'].dt.strftime('%H:%M:%S') \
                                            .astype('datetime64[ns]')

    # Insert in close_approach_data the 'count_per_date' column with the count of NEOs per date
    close_approach_data['count_per_date'] = close_approach_data.groupby('approach_date')['neo_reference_id'] \
                                            .transform('count') \
                                            .astype('int16')

    schemas = read_json(schemas_path) # Read the schemas file for the transformation

    # Merge close_approach_data and asteroid_data selecting columns from the schemas file
    join_schema = sorted(schemas['near_earth_approaches'], key=lambda col: col['column_position'])
    join_columns = [col['column_name'] for col in join_schema]

    near_earth_approaches = inner_join(asteroid_data, close_approach_data,
                                      condition='neo_reference_id', select=join_columns)

    # Loading: near_earth_approaches
    save_new_data(near_earth_approaches,
                  f'{tgt_path}/near_earth_approaches',
                  predicate=f'''tgt.approach_date = src.approach_date
                    AND tgt.neo_reference_id = src.neo_reference_id''',
                  partition_cols=['year', 'month', 'week'])


if __name__ == '__main__':
    # Read the .conf configuration file
    config_file = 'config.conf'
    parser = ConfigParser()
    parser.read(config_file)

    # Configuration of basic parameters for the API request
    base_url = parser['source']['url']
    token = parser['source']['token']

    if not token:
        raise Exception('Add a valid API token to the .conf file')

    default_params = {
        'api_key': token
    }

    # Configuration of the incremental parameters based on the last date registered or on the present one, if none
    stateful_path = 'metadata/stateful.json'
    current_state = get_state(stateful_path, 'close_approach_data', 'last_value', '%Y-%m-%d')
    update_state = current_state + timedelta(days=7)

    incremental_params = {
        'start_date': current_state.strftime('%Y-%m-%d'),
        'end_date': update_state.strftime('%Y-%m-%d')
    }

    # Paths to save data in different stages
    bronze_path = parser['target']['bronze']
    silver_path = parser['target']['silver']
    gold_path = parser['target']['gold']
    
    # Path of the schemas file
    schemas_path = 'metadata/schemas.json'

    # ELT pipeline
    extract_and_load(bronze_path, base_url, incremental_params, default_params)
    cleaning_transform(bronze_path, silver_path, schemas_path) 
    analytic_transform(silver_path, gold_path, schemas_path)

    # Write the last date requested in the stateful file
    write_state(update_state.strftime('%Y-%m-%d'), stateful_path, 'close_approach_data', 'last_value')
