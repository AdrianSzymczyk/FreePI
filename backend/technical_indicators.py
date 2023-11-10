import sqlite3
from pathlib import Path
import numpy as np
from config import config
import pandas as pd
from typing import Union, List
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


def calculate_RSI(symbol: str, window: int = 14, adjust: bool = False, append: bool = True, data: pd.DataFrame = None) \
        -> pd.DataFrame:
    """
    Calculate Relative Strength Index (RSI) values for given data=
    :param symbol: Stock market symbol
    :param window: The number of periods over which the RSI calculation should be performed
    :param adjust: Bool value passed to 'ewm' method
    :param append: Determine whether return data or append to the database table
    :param data: DataFrame with stock symbol data. Default None
    :return: Pandas DataFrame with data and extra RSI column
    """
    # Fetch the data from the database and reverse for RSI calculation
    if data is None:
        data: pd.DataFrame = receiver.receive_data(symbol)[::-1]
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
    # Append data to the symbol table in the database
    data.loc[:, 'RSI'] = round(RSI, 2)
    if append:
        # Append new data to the database table
        append_to_table(symbol, data[::-1])
        # Set Date as index in the DataFrame
        data.set_index('Date', inplace=True)
        return data[::-1]
    else:
        # Set Date as index in the DataFrame
        data.set_index('Date', inplace=True)
        return data[::-1]


def calculate_MACD(symbol: str, fast_period: int = 12, slow_period: int = 26, signal_period: int = 9,
                   append=True, data: pd.DataFrame = None) -> pd.DataFrame:
    """
    Calculate Moving Average Convergence Divergence (MACD) values for given data
    :param symbol: Stock market symbol
    :param fast_period: The number of periods for the short-term
    :param slow_period: The number of periods for the long-term
    :param signal_period: The number of periods for the Signal Line
    :param append: Determine whether return data or append to the database table
    :param data: DataFrame with stock symbol data. Default None
    :return: Pandas DataFrame with data and extra MACD columns
    """
    # Fetch the data from the database and reverse for MACD calculation
    if data is None:
        data: pd.DataFrame = receiver.receive_data(symbol)
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
    data['MACD_Line'] = round(macd_line, 2)
    data['MACD_Signal'] = round(signal_line, 2)
    data['MACD_Hist'] = round(histogram, 2)
    if append:
        # Append new data to the database table
        append_to_table(symbol, data)
        # Set Date as index in the DataFrame
        data.set_index('Date', inplace=True)
        return data
    else:
        # Set Date as index in the DataFrame
        data.set_index('Date', inplace=True)
        return data


def calculate_EMA(symbol: str, period: int = 10, append: bool = True, data: pd.DataFrame = None) -> pd.DataFrame:
    """
    Calculate Exponential Moving Average Indicator (EMA) values for given data
    :param symbol: Stock market symbol
    :param period: The number of periods over which the EMA calculation is performed
    :param append: Determine whether return data or append to the database table
        :param data: DataFrame with stock symbol data. Default None
    :return: Pandas DataFrame with data and extra EMA column
    """
    # Fetch the data from the database and reverse for EMA calculation
    if data is None:
        data: pd.DataFrame = receiver.receive_data(symbol)
    # Calculate Exponential Moving Average Indicator
    ema = data['Close'].ewm(span=period, adjust=False).mean()
    data[f'EMA_{period}'] = round(ema, 2)
    if append:
        # Append new data to the database table
        append_to_table(symbol, data)
        # Set Date as index in the DataFrame
        data.set_index('Date', inplace=True)
        return data
    else:
        # Set Date as index in the DataFrame
        data.set_index('Date', inplace=True)
        return data


def calculate_SMA(symbol: str, period: int = 14, append: bool = True, data: pd.DataFrame = None) -> pd.DataFrame:
    """
    Calculate Simple Moving Average Indicator (SMA) values for given data
    :param symbol: Stock market symbol
    :param symbol: Stock market symbol
    :param period: The number of periods over which the SMA calculation is performed
    :param append: Determine whether return data or append to the database table
    :param data: DataFrame with stock symbol data. Default None
    :return: Pandas DataFrame with data and extra SMA column
    """
    # Fetch the data from the database and reverse for SMA calculation
    if data is None:
        data: pd.DataFrame = receiver.receive_data(symbol)
    # Calculate Simple Moving Average Indicator
    sma = data['Close'].rolling(window=period, min_periods=1).mean()
    data[f'SMA_{period}'] = round(sma, 2)
    if append:
        # Append new data to the database table
        append_to_table(symbol, data)
        # Set Date as index in the DataFrame
        data.set_index('Date', inplace=True)
        return data
    else:
        # Set Date as index in the DataFrame
        data.set_index('Date', inplace=True)
        return data


def calculate_PSAR(symbol: str, af_start=0.02, af_increment=0.02, af_max=0.2, append=True) -> pd.DataFrame:
    """
        Calculate Parabolic SAR Indicator (PSAR) values for given data
        :param symbol: Stock market symbol
        :param af_start:
        :param af_increment:
        :param af_max:
        :param append: Determine whether return data or append to the database table
        :return: Pandas DataFrame with data and extra PSAR column
        """
    # Fetch the data from the database and reverse for RSI calculation
    data: pd.DataFrame = receiver.receive_data(symbol)
    high_prices = data['High']
    low_prices = data['Low']

    # Initialize variables
    af: float = af_start
    psar: List[float] = []
    uptrend: bool = True
    extreme_high = high_prices.iloc[0]
    extreme_low = low_prices.iloc[0]
    sar = low_prices.iloc[0] if uptrend else high_prices.iloc[0]

    for i in range(len(data)):
        if uptrend:
            if high_prices.iloc[i] > extreme_high:
                extreme_high = high_prices.iloc[i]
                af = min(af + af_increment, af_max)

            sar = sar + af * (extreme_high - sar)

            if low_prices.iloc[i] < sar:
                uptrend = False
                sar = extreme_high
                extreme_low = low_prices.iloc[i]
                af = af_start

        else:
            if low_prices.iloc[i] < extreme_low:
                extreme_low = low_prices.iloc[i]
                af = min(af + af_increment, af_max)

            sar = sar - af * (sar - extreme_low)

            if high_prices.iloc[i] > sar:
                uptrend = True
                sar = extreme_low
                extreme_high = high_prices.iloc[i]
                af = af_start

        psar.append(sar)

    print(pd.Series(psar, index=data.index))
    # return pd.Series(psar, index=df.index)


def get_indicator(symbol: str, indicator: str, period: int = 14, fast_period: int = 12, slow_period: int = 26,
                  signal_period: int = 9) -> pd.DataFrame:
    """
    Get the specific indicator for stock symbol.
    :param symbol: Stock market symbol
    :param indicator: Name of the technical indicator
    :param period: The number of periods over which the indicator calculation is performed
    :param fast_period: The number of periods for the short-term
    :param slow_period: The number of periods for the long-term
    :param signal_period: The number of periods for the Signal Line
    :return: Pandas DataFrame with indicator data
    """
    # Check whether the parameters are default
    non_default_params: bool = False
    if period != 14 or fast_period != 12 or slow_period != 26 or signal_period != 9:
        non_default_params = True
    # Available indicators
    available_indicators: List[str] = ['MACD', 'RSI', 'EMA', 'SMA']
    # Get the name of the symbol table
    table_name = app.get_name_of_symbol_table(symbol=symbol, frequency='1d')
    if table_name is not None:
        append_new_indicator: bool = False
        if not non_default_params:
            data = receiver.receive_data(symbol, change_index=True)
            return_column: List[str] = [col for col in data.columns if indicator in col]
            if len(return_column) != 0:
                return data[return_column]
            # Indicate the methods to append the new column with indicator
            append_new_indicator = True
        if indicator in available_indicators:
            if indicator == 'MACD':
                data = calculate_MACD(symbol, fast_period, slow_period, signal_period, append=append_new_indicator)
            elif indicator == 'RSI':
                data = calculate_RSI(symbol, window=period, append=append_new_indicator)
            elif indicator == 'EMA':
                data = calculate_EMA(symbol, period=period, append=append_new_indicator)
            elif indicator == 'SMA':
                data = calculate_SMA(symbol, period=period, append=append_new_indicator)
            else:
                data = receiver.receive_data(symbol, change_index=True)
            return_column: List[str] = [col for col in data.columns if indicator in col]
            return data[return_column]
        else:
            print(f'Given indicator [{indicator}] is not handled for {symbol}!')


def update_single_symbol(connection: sqlite3.Connection, symbol: str, database_name: str = 'stock_database.db') -> None:
    """
    Update the technical indicators for a single stock symbol
    :param connection: Connection to the SQLite database
    :param symbol: Stock market symbol
    :param database_name: Name of the database where data will be saved. Default "stock_database"
    """
    # Get the name of symbol table
    table_name = app.get_name_of_symbol_table(symbol, '1d', connection, database_name)
    if table_name is not None:
        # Fetch all the table columns
        column_exists = connection.execute(f'PRAGMA table_info(`{table_name}`);')
        table_columns = [col[1] for col in column_exists]
        # Create a lists with all the SMA, EMA periods
        ema_periods = [ema for ema in table_columns if 'EMA' in ema]
        sma_periods = [sma for sma in table_columns if 'SMA' in sma]

        # Receive data for calculating indicators
        data: pd.DataFrame = receiver.receive_data(symbol, database_name=database_name)
        # Execute all the indicator methods
        calculate_RSI(symbol, data=data)  # Calculate RSI
        calculate_MACD(symbol, data=data)  # Calculate MACD
        if len(ema_periods) != 0:  # Calculate all the EMA's
            for ema in ema_periods:
                calculate_EMA(symbol, int(ema.split('_')[1]), data=data)
        else:
            calculate_EMA(symbol, data=data)

        if len(sma_periods) != 0:  # Calculate all the SMA's
            for sma in sma_periods:
                calculate_SMA(symbol, int(sma.split('_')[1]), data=data)
        else:
            calculate_SMA(symbol, data=data)


def update_indicators(symbols: Union[str, List[str], np.ndarray], database_name: str = 'stock_database.db') -> None:
    """
    Update the technical indicators for given symbols
    :param symbols: Stock market symbols
    :param database_name: Name of the database where data will be saved. Default "stock_database"
    """
    # Create connection with the database
    conn = sqlite3.connect(Path(config.DATA_DICT, database_name))
    # Update indicators for the single symbol
    if isinstance(symbols, str):
        update_single_symbol(conn, symbols, database_name)
    # Update indicators for the list of symbols
    elif isinstance(symbols, list):
        for symbol in symbols:
            update_single_symbol(conn, symbol, database_name)
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

    # print(get_indicator('TSLA', 'RSI', period=200))
    print(get_indicator('TSLA', 'SMA', period=14).head(20))
