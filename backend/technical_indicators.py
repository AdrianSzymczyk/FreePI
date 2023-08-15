import sqlite3
from pathlib import Path
from config import config
import pandas as pd
from typing import Union, Tuple, List
from webScrape import app, receiver


def append_to_table(symbol: str, data: Union[pd.Series, Tuple], column_name: Union[str, List[str]]):
    conn = sqlite3.connect(Path(config.DATA_DICT, 'stock_database.db'))
    table_name = app.get_name_of_symbol_table(symbol=symbol, frequency='1d', connection=conn)
    if table_name is not None:
        # Fetch all the table columns
        column_exists = conn.execute(f'PRAGMA table_info(`{table_name}`);')
        column_names = [col[1] for col in column_exists]
        if isinstance(data, pd.Series):
            # Drop column if exists
            if column_name in column_names:
                conn.execute(f"ALTER TABLE `{table_name}` DROP COLUMN {column_name};")
            # Create a new column without the data
            conn.execute(f'ALTER TABLE `{table_name}` ADD {column_name} REAL;')
            # Insert data for the new column to the table,
            cursor = conn.cursor()
            for date, element in data.items():
                insert_query: str = f"UPDATE `{table_name}` SET {column_name} = {round(element, 4)} WHERE Date = '{date}';"
                cursor.execute(insert_query)
            conn.commit()
            app.fetch_from_database('NVDA', '1d')
        else:
            for series, column in zip(data, column_name):
                # Drop column if exists
                if column in column_names:
                    conn.execute(f"ALTER TABLE `{table_name}` DROP COLUMN {column_name}")
                # Append new column to the table
                series.to_sql(table_name, conn, if_exists='append', index=False, index_label=column)
    else:
        print('NO WAY!')
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
        append_to_table(symbol, RSI, 'RSI')
        data.loc[:, 'RSI'] = round(RSI, 4)
        return data[::-1]
    return RSI


def calculate_MACD(symbol, fast_period: int = 12, slow_period: int = 26, signal_period: int = 9,
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

    if append:
        data['MACD_Line'] = round(macd_line, 4)
        data['Signal_Line'] = round(signal_line, 4)
        data['Histogram'] = round(histogram, 4)
        return data
    else:
        return macd_line, signal_line, histogram


def calculate_EMA(symbol: str, period: int = 14, append: bool = True) -> Union[pd.Series, pd.DataFrame]:
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
        data[f'EMA_{period}'] = round(ema, 4)
        return data
    else:
        return ema


df = pd.read_csv(Path(Path(__file__).parent.parent.absolute(), 'Indicators_test.csv'), index_col='Date')

# result = calculate_MACD(df)
result = calculate_RSI('NVDA')
# print(result)
# df = calculate_RSI(df)
# df = calculate_MACD(df)

# print(df.head(40))
