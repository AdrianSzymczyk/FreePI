import sqlite3
from datetime import datetime
import pandas as pd
from webScrape import app
from typing import List, Tuple
from pathlib import Path
from config import config


def receiver(connection: sqlite3.Connection, symbol_table_name: str, start_date: datetime.date, end_date: datetime.date,
             change_index: bool = False) -> pd.DataFrame:
    """
    Return stock data from the database symbol table
    :param connection: Connection to the SQLite database
    :param symbol_table_name: Name of the stock symbol table
    :param start_date: Beginning of the period of time, valid format: "2021-09-08"
    :param end_date: End of the period of time, valid format: "2021-08-08"
    :param change_index: Whether to set date as indices in data
    :return: Pandas DataFrame with stock data from a date range
    """
    # Query to fetch symbol data from the database
    fetch_query = f"""
            SELECT * 
            FROM `{symbol_table_name}`
            WHERE Date BETWEEN '{start_date}' AND '{end_date}'
            ORDER BY Date DESC
            """
    # Fetch all data from the date range and save to a variable
    cursor = connection.execute(fetch_query)
    result: List[Tuple[str, str, str, str, str, str, str]] = cursor.fetchall()

    # Create and save data inside the Pandas DataFrame
    column_exists = connection.execute(f'PRAGMA table_info(`{symbol_table_name}`);')
    column_names = [col[1] for col in column_exists]
    df_symbol: pd.DataFrame = pd.DataFrame(result, columns=column_names)
    if change_index:
        df_symbol.set_index('Date', inplace=True)

    try:
        # Convert numeric columns to appropriate data type
        numeric_columns: List[str] = ['Open', 'High', 'Low', 'Close', 'Adj Close']
        df_symbol[numeric_columns] = df_symbol[numeric_columns].astype(float)

        # Remove commas from "Volume" column and convert to integer
        df_symbol['Volume'] = df_symbol['Volume'].str.replace(',', '').astype(int)
    except AttributeError:
        pass

    return df_symbol


def receive_data(symbol: str, connection: sqlite3.Connection | None = None, start: str = '1972-06-02',
                 end: str = datetime.strftime(datetime.now().date(), '%Y-%m-%d'),
                 frequency: str = '1d', change_index: bool = False,
                 database_name: str = 'stock_database.db') -> pd.DataFrame:
    """
    Return data from a date range from a specific stock symbol
    :param symbol: Stock market symbol
    :param connection: Connection to the SQLite database
    :param start: Beginning of the period of time, valid format: "2021-09-08"
    :param end: End of the period of time, valid format: "2021-08-08"
    :param frequency: String defining the frequency of the data, defaults-1d, possible values: [1d, 1wk, 1mo]
    :param change_index: Whether to set date as indices in data
    :param database_name: Name of the database where data will be saved. Default "stock_database"
    :return: Pandas DataFrame with stock data from a date range
    """
    # Convert passed start and end dates into datetime.date format
    received_data: pd.DataFrame = pd.DataFrame()
    start_date: datetime.date = datetime.strptime(start, '%Y-%m-%d').date()
    end_date: datetime.date = datetime.strptime(end, '%Y-%m-%d').date()

    new_connection: bool = False
    limit_date: datetime.date = datetime.strptime('1972-06-02', '%Y-%m-%d').date()
    # Connect to the database
    if connection is None:
        new_connection = True
        connection = sqlite3.connect(f'{Path(config.DATA_DICT, database_name)}')
    # Get the name of the symbol table
    symbol_table_name: str = app.get_name_of_symbol_table(symbol, frequency, connection)
    if symbol_table_name is not None:
        # Check whether the receiver date range is covered by existing data
        table_start, table_end = app.extract_date_from_table(symbol_table_name)
        if ((start_date < table_start and 'oldest' not in symbol_table_name.split('_')[1])
                or end_date > table_end
                or (table_start > limit_date and 'oldest' not in symbol_table_name.split('_')[1])):
            app.download_historical_data(symbol, start, end, frequency)
            # Update table name
            symbol_table_name = app.get_name_of_symbol_table(symbol, frequency, connection)
        received_data = receiver(connection, symbol_table_name, start_date, end_date, change_index)
    else:
        app.download_historical_data(symbol, start, end, frequency)
        # Get the name of the symbol table
        symbol_table_name = app.get_name_of_symbol_table(symbol, frequency, connection)
        if symbol_table_name is not None:
            received_data = receiver(connection, symbol_table_name, start_date, end_date, change_index)
    if new_connection:
        connection.close()
    return received_data


if __name__ == "__main__":
    # print(receive_data('NVDA', '2020-01-01', '2023-07-08'))
    # print(receive_data('TSLA', '2009-01-01', '2023-07-12'))
    # print(receive_data('AAPL', '2021-01-01', '2023-07-12'))
    # print(receive_data('XZAA', '2021-01-01', '2023-07-12'))
    # print(receive_data('TSLA', '2019-01-01', '2023-09-12'))
    print(receive_data('NVDA', frequency='1mo', change_index=True))
    # app.display_database_tables()
