from deltalake import DeltaTable
from deltalake.exceptions import DeltaError
from deltalake.table import TableOptimizer
from configparser import ConfigParser
import os


def compact_table(path: str, retention_days: int):
    """
    Function:
    Optimize Delta table storage by compacting parquet files and
    deleting them if they are older than the set daily amount.

    Args:
    path (str): Path to the Delta table directory.
    retention_days (int): Retention threshold in days.
    """
    try:
        dt = DeltaTable(path)
        TableOptimizer(dt).compact()
        enforce = True if retention_days != 0 else False # If the retention time is 0 days (0 hours), it is suddenly vacuumed
        dt.vacuum(retention_hours=retention_days * 24, dry_run=False, enforce_retention_duration=enforce)

    except DeltaError as e:
        print('Error in the compact or vacuum operation:', e)


def z_order_table(path: str, ordering_columns: list):
    """
    Function:
    Optimize the reading of the Delta Table by grouping records by 
    specific columnar values, reducing data shuffling.

    Args:
    path (str): Path to the Delta Table directory.
    ordering_columns (list): Columns to group rows by.
    """
    try:
        dt = DeltaTable(path)
        dt.optimize.z_order(columns=ordering_columns)
    
    except DeltaError as e:
        print('Error in the z-order operation:', e)


if __name__ == '__main__':
    # Read the .conf configuration file
    config_file = 'config.conf'
    parser = ConfigParser()
    parser.read(config_file)

    # Paths to save data in different stages
    bronze_path = parser['target']['bronze']
    silver_path = parser['target']['silver']
    gold_path = parser['target']['gold']

    # Optimize each table in each stage
    for stage in [bronze_path, silver_path, gold_path]:
        for dir in os.listdir(stage):
            path = f'{stage}/{dir}'
            compact_table(path, retention_days=7)