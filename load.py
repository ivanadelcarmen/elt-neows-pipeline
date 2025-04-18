from deltalake import write_deltalake, DeltaTable
from deltalake.exceptions import TableNotFoundError
import pyarrow as pa
import pandas as pd
from typing import Union


def save_data(df: pd.DataFrame, path: str, mode: str="overwrite", partition_cols: Union[None, list]=None):
    """
    Function:
    Saves a dataframe in Delta Lake format to the specified path.
    It can also partition the dataframe by one or more columns.
    By default, the save mode is "overwrite".

    Args:
    df (pd.DataFrame): The dataframe to save
    path (str): The path where the dataframe will be saved in Delta Lake format
    mode (str): The save mode. These are the modes supported by the Delta Lake library: \
    "overwrite", "append", "error", "ignore".
    partition_cols (list or str): The column(s) by which the \
    dataframe will be partitioned. If not specified, it will not be partitioned.
    """
    write_deltalake(
        path, df, mode=mode, partition_by=partition_cols,
        configuration= {
            'delta.logRetentionDuration': 'interval 7 days',
            'delta.deletedFileRetentionDuration': 'interval 7 days'
        } # Configurations for the table compaction
    )


def save_new_data(new_data: pd.DataFrame, path: str, predicate: str, partition_cols: Union[None, list]=None):
    """
    Function:
    Saves only new data in Delta Lake format using the MERGE operation,
    comparing the already loaded data with the data to store,
    ensuring that no duplicate records are saved.

    Args:
    new_data (pd.DataFrame): The data you want to save.
    path (str): The path where the data frame will be saved in Delta Lake format.
    predicate (str): The predicate condition for the MERGE operation.
    partition_cols (list): A list object with the names of the columns to partition the table.
    """
    try:
        dt = DeltaTable(path)
        new_data_pa = pa.Table.from_pandas(new_data)
        dt.merge(
            source=new_data_pa,
            source_alias="src",
            target_alias="tgt",
            predicate=predicate
        ) \
            .when_not_matched_insert_all() \
            .execute()
        
    except TableNotFoundError:
        save_data(new_data, path, partition_cols=partition_cols)


def upsert_data(data: pd.DataFrame, path: str, predicate: str):
    """
    Function:
    Save data in Delta Lake format using the MERGE operation.
    When there are no matching records, new records will be inserted.
    When there are matching records, the fields will be updated.

    Args:
    data (pd.DataFrame): The data to be saved.
    path (str): The path where the data frame will be saved in Delta Lake format.
    predicate (str): The predicate condition for the MERGE operation.
    """
    try:
        dt = DeltaTable(path)
        data_pa = pa.Table.from_pandas(data)
        dt.merge(
            source=data_pa,
            source_alias="src",
            target_alias="tgt",
            predicate=predicate
        ) \
            .when_matched_update_all() \
            .when_not_matched_insert_all() \
            .execute()

    except TableNotFoundError:
        save_data(data, path)