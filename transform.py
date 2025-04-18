import pandas as pd
import re
from typing import Union


def filter_columns(df: pd.DataFrame, columns: list) -> pd.DataFrame:
    """
    Function:
    Deletes columns from a table based on a selection of columns

    Args:
    df (pd.DataFrame): The DataFrame to modify.
    columns (list): Sequence of columns to select and keep.

    Returns:
    pd.DataFrame: A DataFrame object with the modification.
    """
    try:
        deletion_columns = [col for col in df.columns if col not in columns]
        new_df = df.drop(deletion_columns, axis=1)
        return new_df

    except KeyError:
        print('At least one element is not in the DataFrame with columns:\n', list(df.columns))
        return pd.DataFrame()


def rename_columns(df: pd.DataFrame, columns: list, table: str) -> pd.DataFrame:
    """
    Function:
    Renames table columns based on a selection of columns and their modified names using regular expressions.

    Args:
    df (pd.DataFrame): The DataFrame to modify.
    columns (list): Sequence of columns with the original names.
    renamed_columns (list): Sequence of columns with the new names. Must be the same length as 'columns'.
    table (str): Indicates the table to which the modification is being made to determine the regular expression.

    Returns:
    pd.DataFrame: A DataFrame object with the respective modification.
    """
    try:
        # For close_approach_data, take the text behind the first period and for asteroid_data take the text after the last period
        pattern = r"^[^.]+" if table == 'close_approach_data' else r"[^.]+$"

        renamed_columns = [re.findall(pattern, col)[0] for col in columns]
        columns_pair = dict(zip(columns, renamed_columns))
        new_df = df.rename(columns=columns_pair, inplace=False)
        return new_df

    except KeyError:
        print(f'At least one element is not in the DataFrame with columns:\n', list(df.columns))
        return pd.DataFrame()

    except Exception as e:
        print(f'Error:', e)
        return pd.DataFrame()


def cast_columns(df: pd.DataFrame, columns: list, dtypes: list) -> pd.DataFrame:
    """
    Function:
    Modifies the type of a table's columns based on a selection of columns and their data.

    Args:
    df (pd.DataFrame): The DataFrame to modify.
    columns (list): Sequence of referenced columns.
    dtypes (list): Sequence of the data types of each column, in order. Must be the same length as 'columns'.

    Returns:
    pd.DataFrame: A DataFrame object with the respective modification.
    """
    try:
        # Create a dictionary with (column: dtype) format
        type_mapping = {col: dtypes[idx] for idx, col in enumerate(columns)}
        new_df = df.astype(type_mapping)
        return new_df

    except TypeError as e:
        print(f'Parameters are incorrect:', e)
        return pd.DataFrame()


def clean_table(df: pd.DataFrame, schemas: dict, table: str) -> pd.DataFrame:
    """
    Function:
    Cleans a table using column filtering, column renaming, and column casting functions, 
    along with additional modifications to optimize its layout.

    Args:
    df (pd.DataFrame): The DataFrame to modify.
    schemas (dict): A dict object where the table schemas are stored.
    table (str): Name of the table referenced in the schemas.

    Returns:
    pd.DataFrame: A DataFrame object with the respective modifications.
    """
    try:
        # Configuration of the sorted schema parameters
        table_schema = sorted(schemas[table], key=lambda col: col['column_position'])
        col_names = [col['column_name'] for col in table_schema]
        col_types = [col['column_type'] for col in table_schema]

        transformed_table = filter_columns(df, col_names)
        transformed_table = cast_columns(transformed_table, col_names, col_types)
        transformed_table = rename_columns(transformed_table, col_names, table)

        return transformed_table

    except Exception as e:
        print('Error:', e)
        return pd.DataFrame()


def inner_join(df1: pd.DataFrame, df2: pd.DataFrame, \
               condition: Union[list, str], select: Union[None, list]=None) -> pd.DataFrame:
    """
    Function:
    Performs an INNER JOIN between two tables given certain conditions and selects specific columns if necessary.

    Args:
    df1 (pd.DataFrame): The left DataFrame.
    df2 (pd.DataFrame): The right DataFrame.
    condition (str) | (list): Column(s) on which to perform the JOIN operation.
    select (list): Sequence of selected columns.

    Returns:
    pd.DataFrame: A DataFrame object with the aforementioned operations.
    """
    try:
        new_df = pd.merge(df1, df2, how='inner', on=condition)
        if select:
            new_df = new_df[select]
        return new_df

    except TypeError as e:
        print('Parameters are not correct:', e)
        return pd.DataFrame()

    except Exception as e:
        print('Error:', e)
        return pd.DataFrame()
