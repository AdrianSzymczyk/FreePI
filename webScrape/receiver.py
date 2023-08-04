import sqlite3
from datetime import datetime
import pandas as pd
from webScrape import app
from typing import List, Tuple
from pathlib import Path
from config import config


def receiver(connection, symbol_table_name, start_date, end_date):
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
    columns: List[str] = ['Date', 'Open', 'High', 'Low', 'Close*', 'Adj Close**', 'Volume']
    df_symbol: pd.DataFrame = pd.DataFrame(result, columns=columns)

    # Close connection with database
    connection.close()

    return df_symbol


def receive_data(symbol: str, start: str, end: str, frequency: str = '1d') -> pd.DataFrame:
    """
    Return data from a date range from a specific stock symbol
    :param symbol: Stock market symbol
    :param start: Beginning of the period of time, valid format: "2021-09-08"
    :param end: End of the period of time, valid format: "2021-08-08"
    :param frequency: String defining the frequency of the data, defaults-1d, possible values: [1d, 1wk, 1mo]
    :return: Pandas DataFrame with stock data from a date range
    """
    # Convert passed start and end dates into datetime.date format
    start_date: datetime.date = datetime.strptime(start, '%Y-%m-%d').date()
    end_date: datetime.date = datetime.strptime(end, '%Y-%m-%d').date()

    # Connect to the database
    conn = sqlite3.connect(f'{Path(config.DATA_DICT, "stock_database.db")}')
    # Get the name of the symbol table
    symbol_table_name: str = app.get_name_of_symbol_table(symbol, frequency, conn)
    if symbol_table_name is not None:
        return receiver(conn, symbol_table_name, start_date, end_date)
    else:
        app.download_historical_data(symbol, start, end, frequency)
        # Change the name of the symbol table
        symbol_table_name = app.get_name_of_symbol_table(symbol, frequency, conn)
        if symbol_table_name is not None:
            return receiver(conn, symbol_table_name, start_date, end_date)


if __name__ == "__main__":
    # print(receive_data('NVDA', '2020-01-01', '2023-07-08'))
    print(receive_data('TSLA', '2009-01-01', '2023-07-12'))
    print(receive_data('AAPL', '2021-01-01', '2023-07-12'))
    app.display_database_tables()
