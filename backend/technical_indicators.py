import sqlite3
from pathlib import Path
import numpy as np
from config import config
import pandas as pd
from typing import Union, Tuple, List
from webScrape import app, receiver


def append_to_table(symbol: str, data: pd.DataFrame) -> None:
    """
    Append new columns into stock symbol table
    :param symbol: Stock market symbol
    :param data: Pandas DataFrame with the data to be saved in table
    """
    conn = sqlite3.connect(Path(config.DATA_DICT, 'stock_database.db'))
    table_name = app.get_name_of_symbol_table(symbol=symbol, frequency='1d', connection=conn)
    if table_name is not None:
        data.to_sql(table_name, conn, if_exists='replace', index=False)
    else:
        pass
    conn.close()


def calculate_RSI(symbol: str, window: int = 14, adjust: bool = False, append: bool = True) \
        -> Union[pd.DataFrame, pd.Series]:
    """
    Calculate relative strength index (RSI) values for given data
    :param symbol: Stock market symbol
    :param window: The number of periods over which the RSI calculation should be performed
    :param adjust: Bool value passed to 'ewm' method
    :param append: Determine whether return data or append to the database table
    :return: Pandas DataFrame with data and extra column or Pandas Series with RSI values
    """
    # Fetch the data from the database and reverse for RSI calculation
    data = receiver.receive_data(symbol)[::-1]
    delta = data['Close'].diff(1).dropna()
    loss = delta.copy()
    gains = delta.copy()

    gains[gains < 0] = 0
    loss[loss > 0] = 0

    gain_ema = gains.ewm(com=window - 1, adjust=adjust).mean()
    loss_ema = abs(loss.ewm(com=window - 1, adjust=adjust).mean())

    RS = gain_ema / loss_ema
    RSI = 100 - 100 / (1 + RS)

    RSI = RSI[::-1]
    if append:
        data.loc[:, 'RSI'] = round(RSI, 2)
        append_to_table(symbol, data[::-1])
        # Set Date as index in the DataFrame
        data.set_index('Date', inplace=True)
        return data[::-1]
    return RSI


def calculate_MACD(symbol: str, fast_period: int = 12, slow_period: int = 26, signal_period: int = 9,
                   append=True) -> Union[pd.DataFrame, Tuple[pd.Series, pd.Series, pd.Series]]:
    """
    Calculate moving average convergence divergence (MACD) values for given data
    :param symbol: Stock market symbol
    :param fast_period: The number of periods for the short-term
    :param slow_period: The number of periods for the long-term
    :param signal_period: The number of periods for the Signal Line
    :param append: Determine whether return data or append to the database table
    :return: Pandas DataFrame with data and extra column or tuple with MACD, signal and histogram values
    """
    # Fetch the data from the database and reverse for RSI calculation
    data = receiver.receive_data(symbol)
    # Calculate the Short-term EMA (fast EMA)
    short_ema = data['Close'].ewm(span=fast_period, adjust=False).mean()
    # Calculate the Long-term EMA (slow EMA)
    long_ema = data['Close'].ewm(span=slow_period, adjust=False).mean()
    # Calculate the MACD line
    macd_line = short_ema - long_ema
    # Calculate the Signal line (9-day EMA of MACD line)
    signal_line = macd_line.ewm(span=signal_period, adjust=False).mean()
    # Calculate the MACD histogram
    histogram = macd_line - signal_line
    # Append data to the symbol table in the database
    if append:
        data['MACD_Line'] = round(macd_line, 2)
        data['MACD_Signal'] = round(signal_line, 2)
        data['MACD_Hist'] = round(histogram, 2)
        append_to_table(symbol, data)
        # Set Date as index in the DataFrame
        data.set_index('Date', inplace=True)
        return data
    else:
        return macd_line, signal_line, histogram


def calculate_EMA(symbol: str, period: int = 14, append: bool = True) -> pd.Series:
    """
    Calculate moving average (EMA) values for given data
    :param symbol: Stock market symbol
    :param period: The number of periods over which the EMA calculation is performed
    :param append: Determine whether return data or append to the database table
    :return: Pandas DataFrame with data and extra column or Pandas Series with EMA
    """
    # Fetch the data from the database and reverse for RSI calculation
    data = receiver.receive_data(symbol)
    ema = data['Close'].ewm(span=period, adjust=False).mean()
    if append:
        data[f'EMA_{period}'] = round(ema, 2)
        append_to_table(symbol, data)
        # Set Date as index in the DataFrame
        data.set_index('Date', inplace=True)
        return data
    else:
        return ema


def get_indicator(symbol: str, indicator: str) -> pd.DataFrame:
    """
    Get the specific indicator for stock symbol.
    :param symbol: Stock market symbol
    :param indicator: Name of the technical indicator
    :return: Pandas DataFrame with indicator data
    """
    # Available indicators
    available_indicators: List[str] = ['MACD', 'RSI', 'EMA']
    # Connect to the database
    conn = sqlite3.connect(Path(config.DATA_DICT, 'stock_database.db'))
    # Get the name of the symbol table
    table_name = app.get_name_of_symbol_table(symbol=symbol, frequency='1d', connection=conn)
    # Close the connection with the database
    conn.close()
    if table_name is not None:
        data = receiver.receive_data(symbol, change_index=True)
        return_column: List[str] = [col for col in data.columns if indicator in col]
        if len(return_column) != 0:
            return data[return_column]
        else:
            if indicator in available_indicators:
                if indicator == 'MACD':
                    data = calculate_MACD(symbol)
                elif indicator == 'RSI':
                    data = calculate_RSI(symbol)
                elif indicator == 'EMA':
                    data = calculate_EMA(symbol)
                return_column: List[str] = [col for col in data.columns if indicator in col]
                return data[return_column]
            else:
                print(f'Given indicator [{indicator}] is not handled for {symbol}!')


# TODO:
#  calculate indicator when API request but symbol doesn't have it


def update_single_symbol(connection: sqlite3.Connection, symbol: str) -> None:
    """
    Update the technical indicators for a single stock symbol
    :param connection: Connection to the SQLite database
    :param symbol: Stock market symbol
    """
    # Get the name of symbol table
    table_name = app.get_name_of_symbol_table(symbol, frequency='1d', connection=connection)
    if table_name is not None:
        # Fetch all the table columns
        column_exists = connection.execute(f'PRAGMA table_info(`{table_name}`);')
        table_columns = [col[1] for col in column_exists]
        # Create a list with all the EMA periods
        ema_periods = [ema for ema in table_columns if 'EMA' in ema]
        # Execute all the indicator methods
        calculate_RSI(symbol)
        calculate_MACD(symbol)
        if len(ema_periods) != 0:
            for ema in ema_periods:
                calculate_EMA(symbol, int(ema.split('_')[1]))
        else:
            calculate_EMA(symbol)


def update_indicators(symbols: Union[str, List[str], np.ndarray]) -> None:
    """
    Update the technical indicators for given symbols
    :param symbols: Stock market symbols
    """
    # Create connection with the database
    conn = sqlite3.connect(Path(config.DATA_DICT, 'stock_database.db'))
    # Update indicators for the single symbol
    if isinstance(symbols, str):
        update_single_symbol(conn, symbols)
    # Update indicators for the list of symbols
    elif isinstance(symbols, list):
        for symbol in symbols:
            update_single_symbol(conn, symbol)
    # Close the database connection
    conn.close()


if __name__ == '__main__':
    pass
    # calculate_RSI('NVDA')
    # calculate_MACD('NVDA')
    # print(get_indicator('NVDA', 'MACD'))
    # print(get_indicator('NVDA', 'RSI'))
    # calculate_EMA('NVDA')
    # get_indicator('NVDA', 'EMA_14')

    # calculate_RSI('TSLA')
    # calculate_MACD('TSLA')
    # update_indicators(['NVDA', 'TSLA'])

    get_indicator('AAPL', 'MACD')
