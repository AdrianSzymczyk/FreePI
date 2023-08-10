import pandas as pd
from pathlib import Path
from typing import Union, Tuple


def calculate_RSI(data: pd.DataFrame, window: int = 14, adjust: bool = False, append: bool = False) \
        -> Union[pd.DataFrame, pd.Series]:
    """
    Calculate relative strength index for given data
    :param data: Pandas DataFrame with data
    :param window: The number of periods over which the RSI calculation should be performed
    :param adjust: Bool value passed to 'ewm' method
    :param append: Determine whether return data or append to given data
    :return: Pandas DataFrame with data and extra column or Pandas Series with RSI values
    """
    reversed_data = data[::-1].copy()
    delta = reversed_data['Close'].diff(1).dropna()
    loss = delta.copy()
    gains = delta.copy()

    gains[gains < 0] = 0
    loss[loss > 0] = 0

    gain_ema = gains.ewm(com=window - 1, adjust=adjust).mean()
    loss_ema = abs(loss.ewm(com=window - 1, adjust=adjust).mean())

    RS = gain_ema / loss_ema
    RSI = 100 - 100 / (1 + RS)

    if append:
        reversed_data.loc[:, 'RSI'] = RSI
        return reversed_data[::-1]
    return RSI


def calculate_MACD(data: pd.DataFrame, fast_period: int = 12, slow_period: int = 26, signal_period: int = 9,
                   append=False) -> Union[pd.DataFrame, Tuple[pd.Series, pd.Series, pd.Series]]:
    """
    Calculate moving average convergence divergence for given data
    :param data: Pandas DataFrame with data
    :param fast_period: The number of periods for the short-term
    :param slow_period: The number of periods for the long-term
    :param signal_period: The number of periods for the Signal Line
    :param append: Determine whether return data or append to given data
    :return: Pandas DataFrame with data and extra column or tuple with MACD, signal and histogram values
    """
    usage_data = data.copy()
    # Calculate the Short-term EMA (fast EMA)
    short_ema = usage_data['Close'].ewm(span=fast_period, adjust=False).mean()

    # Calculate the Long-term EMA (slow EMA)
    long_ema = usage_data['Close'].ewm(span=slow_period, adjust=False).mean()

    # Calculate the MACD line
    macd_line = short_ema - long_ema

    # Calculate the Signal line (9-day EMA of MACD line)
    signal_line = macd_line.ewm(span=signal_period, adjust=False).mean()

    # Calculate the MACD histogram
    histogram = macd_line - signal_line

    if append:
        usage_data['MACD_Line'] = macd_line
        usage_data['Signal_Line'] = signal_line
        usage_data['Histogram'] = histogram
        return usage_data
    else:
        return macd_line, signal_line, histogram


df = pd.read_csv(Path(Path(__file__).parent.parent.absolute(), 'Indicators_test.csv'), index_col='Date')
# print(df.head())

# MACD_line, signal_line, histogram = calculate_MACD(df)
result = calculate_MACD(df)
# df = calculate_RSI(df, append=True)
df = calculate_MACD(df, append=True)

print(df.head(40))
