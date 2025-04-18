import asyncio
import aiohttp
import urllib.parse
import requests
import pandas as pd
from datetime import datetime
from math import ceil
from typing import Union


def get_data(base_url: str, endpoint: str, params: dict, \
             field: Union[None, str]=None, lookup: Union[None, list]=None) -> list:
    """
    Function:
    Makes a GET request to the API synchronously or asynchronously.

    Args:
    base_url (str): The base URL of the API.
    endpoint (str): The API endpoint to which the request will be made.
    params (dict): Query parameters to send with the request.
    field (str): Key of the response JSON dictionary containing the data.
    lookup (list): List of additional parameters in the URL to iterate
    over and request data from multiple URLs.

    Returns:
    list: The data obtained from the API, returned into a list.
    """
    try:
        queries = urllib.parse.urlencode(params)
        if not lookup:
            # Synchronous request
            url = f'{base_url}/{endpoint}?{queries}'
            try:
                response = requests.get(url)
                response.raise_for_status()
                parsed_response = response.json()

                data = parsed_response[field] if field else parsed_response

                if isinstance(data, dict):
                    # Unify the NEOs inside each key (date)
                    data = [entry for list_of_date in data.values() for entry in list_of_date]

            except requests.exceptions.RequestException as e:
                print('Request error:', e)
                return []

            except requests.exceptions.JSONDecodeError as e:
                print("Invalid JSON syntax:", e)

        else:
            # Asynchronous request
            data = []

            async def fetch_all(session, iterable):
                tasks = []
                for num in iterable:
                    url = f'{base_url}/{endpoint}/{num}?{queries}'
                    task = asyncio.create_task(session.get(url, ssl=False))
                    tasks.append(task)
                responses = await asyncio.gather(*tasks)
                return responses

            async def merge_results():
                async with aiohttp.ClientSession() as session:
                    responses = await fetch_all(session, lookup)
                    for item in responses:
                        try:
                            neo = await item.json()
                            data.append(neo)
                        except aiohttp.ContentTypeError as e:
                            print('No se pudo procesar el JSON desde', str(e).split(', ')[2])
                            continue  # If a JSON request and decoding fails, the URL is reported and skipped

            asyncio.run(merge_results())

        return data

    except TypeError as e:
        print('TypeError:', e, 'Insert valid arguments')
        return []


def build_table(entries: list, table: str) -> pd.DataFrame:
    """
    Function:
    Create a table with data in dict format inside a list.

    Args:
    entries (list): Object containing the data as dict objects.
    table (str): The name of the table to generate (asteroid_data or close_approach_data).

    Returns:
    pd.DataFrame: A DataFrame object with the tabulated data.
    """
    try:
        if table == 'asteroid_data':
            rows = []
            columns = ['neo_reference_id', 'name', 'nasa_jpl_url', 'is_potentially_hazardous_asteroid',
                       'absolute_magnitude_h', 'estimated_diameter', 'close_approach_data']

            for entry in entries:
                # Filter the columns for each entry
                row = {col: entry[col] for col in columns if col in entry.keys()}

                # Add 'approaches_to_earth' column with the count of approaches to the Earth registered for each asteroid
                body_approach_list = row.pop('close_approach_data')
                earth_approach_list = list(filter(lambda x: x['orbiting_body'] == 'Earth', body_approach_list))
                row['approaches_to_earth'] = len(earth_approach_list)
                rows.append(row)

            # Normalize JSON (dict) data
            df = pd.json_normalize(rows)

        elif table == 'close_approach_data':
            rows = []
            columns = ['neo_reference_id', 'close_approach_data']

            for entry in entries:
                # Filter the columns for each entry
                row = {col: entry[col] for col in columns if col in entry.keys()}
                rows.append(row)

            # Normalize JSON (dict) data
            df = pd.json_normalize(rows, record_path=table, meta='neo_reference_id')

            # Extract the approach year, month and week of month in separate columns
            df['approach_datetime'] = pd.to_datetime(df['epoch_date_close_approach'], unit='ms') # Convert from Unix Time to datetime
            date_col = df['approach_datetime']

            def week_of_month(date):
                first_day = date.replace(day=1)
                day_of_month = date.day
                adjusted_day_of_month = day_of_month + first_day.weekday()
                return int(ceil(adjusted_day_of_month / 7))

            df['year'] = date_col.dt.year
            df['month'] = date_col.dt.month
            df['week'] = date_col.apply(week_of_month)
            
            # Add 'extraction_date' column with the present execution date
            current_date = datetime.now().strftime('%Y-%m-%d')
            df.insert(len(df.columns), 'extraction_date', current_date)

        else:
            print('The data for the requested table is not available')
            df = pd.DataFrame()

        return df

    except TypeError as e:
        print('TypeError:', e, 'Insert valid arguments')
        return pd.DataFrame()

    except Exception as e:
        print('Error:', e)
        return pd.DataFrame()
