import json
from datetime import datetime, date
from deltalake import DeltaTable
import pandas as pd


def read_json(file_path: str) -> dict:
    """
    Function:
    Reads a JSON file into a dict object.

    Args:
    file_path (str): Path to the JSON file.

    Returns:
    dict: The JSON file converted to a dictionary.
    """
    try:
        with open(file_path, 'r') as file:
            json_file = json.load(file)

        return json_file

    except FileNotFoundError:
        print('File not found:', file_path)
        return {}

    except json.decoder.JSONDecodeError:
        print(f'Could not read file:', file_path.split('/')[-1])
        return {}


def read_delta_table(table_path: str) -> pd.DataFrame:
    """
    Function:
    Reads a directory containing files in Delta Lake format and converts the table to a DataFrame object.

    Args:
    table_path (str): Path to the table in Delta Lake format.

    Returns:
    pd.DataFrame: The table converted to a DataFrame object.
    """
    try:
        df = DeltaTable(table_path).to_pandas()
        if 'extraction_date' in df.columns:
            current_date = datetime.strftime(datetime.now(), '%Y-%m-%d')
            df = df[df['extraction_date'] == current_date] # Select the last extracted batch
        
        return df
    
    except Exception as e:
        print('Error while reading the table:', e)
        return pd.DataFrame()


def get_state(file_path: str, table: str, key: str, date_format: str) -> date:
    """
    Function:
    Gets the stateful parameter for periodic incremental data extraction.

    Args:
    file_path (str): Path to the JSON file.
    table (str): Table referenced in the JSON file.
    key (str): Key for the 'state' value.
    date_format (str): String format of the processed datetime/date objects.

    Returns:
    date: The variable parameter (state).
    """
    try:
        stateful_json = read_json(file_path)
        value = stateful_json[table][key]

        # Take the present execution date if the value is not explicitly written
        value = datetime.strptime(value, date_format) if len(value) != 0 else datetime.now()
        return value

    except KeyError as e:
        print('Could not find key:', e)
        return datetime.date(1900, 1, 1)

    except Exception as e:
        print('Error', e)


def write_state(new_value: str, file_path: str, table: str, key: str):
    """
    Function:
    Writes the new value to the stateful JSON file.

    Args:
    new_value (str): The value of 'state'.
    file_path (str): Path to the JSON file.
    table (str): Table referenced in the JSON file.
    key (str): Key of the 'state' value as a string.
    """
    try:
        current_stateful = read_json(file_path)

        new_stateful = current_stateful.copy()
        new_stateful[table][key] = new_value

        with open(file_path, 'w') as file:
            json.dump(new_stateful, file, indent=4)

    except KeyError as e:
        print('Could not find key:', e)

    except Exception as e:
        print('Error', e)
