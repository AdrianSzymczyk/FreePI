import os
import sqlite3
import time
from typing import List
import numpy as np
import pandas as pd
import pytest
from pyvirtualdisplay import Display

from webScrape import app
import datetime
from pathlib import Path


@pytest.fixture
def data():
    df = pd.DataFrame(
        {
            'Date': ['2010-07-01'],
            'Open': [1.67],
            'High': [1.73],
            'Low': [1.0],
            'Close': [1.33],
            'Adj Close': [1.33],
            'Volume': [968637000]
        }
    )
    return df


@pytest.fixture
def data_directory():
    """Return data absolute path."""
    DEFAULT_DICT = Path(__file__).parent.parent.parent.absolute()
    return DEFAULT_DICT / 'data'


def date_safe_range(date: datetime.date, frequency: str) -> List[datetime.date]:
    """
    Return safe range for given date.
    """
    date_scope: List[datetime.date] = []
    if frequency == '1wk':
        date_scope.append(date - datetime.timedelta(days=7))
        date_scope.append(date + datetime.timedelta(days=7))
    elif frequency == '1mo':
        date_scope.append(date - datetime.timedelta(days=31))
        date_scope.append(date + datetime.timedelta(days=31))
    else:
        date_scope.append(date - datetime.timedelta(days=4))
        date_scope.append(date + datetime.timedelta(days=4))
    return date_scope


@pytest.mark.parametrize(
    'symbol, date_range, frequency, db_save, extra_info',
    [
        (['NKLA', 'SNAP'], '2023-01-12_2023-08-22', '1d', True, ''),
        ('TSLA', '2009-01-01_2023-08-22', '1wk', False, ''),
        ('AAPL', '2010-01-01_2023-08-22', '1mo', True, ''),
        ('AAPL', '2011-05-01_2023-10-05', '1mo', True, 'end_2023-08-22'),
        ('AAPL', '2012-01-01_2023-10-01', '1mo', True, 'inRange'),
        ('AAPL', '2009-05-11_2023-08-31', '1mo', True, 'begin_2010-01-01'),
        ('XYZ', '2009-05-11_2023-08-31', '1mo', False, 'incorrect')
    ]
)
@pytest.mark.scraper
@pytest.mark.download
def test_download_symbol_data(data_directory, symbol, date_range, frequency, db_save, extra_info):
    """Test downloading data and saving into test database."""
    # Check if running in GitHub Actions
    is_github_actions = os.environ.get('GITHUB_ACTIONS') == 'true'
    if is_github_actions:
        # Code to be executed only in GitHub Actions workflow
        display = Display(visible=0, size=(800, 800))
        display.start()

    start_date: str = date_range.split('_')[0]
    end_date: str = date_range.split('_')[1]
    last_date: datetime.date = datetime.datetime.now()
    first_date: datetime.date = datetime.datetime.now()
    start_date_limit: List[datetime.date] = []
    end_date_limit: List[datetime.date] = []

    stock_data: pd.DataFrame = app.download_historical_data(symbol, start_date, end_date, frequency,
                                                            save_database=db_save, database_name=f'{data_directory}\\test_database.db')
    if stock_data is not None:
        last_date = datetime.datetime.strptime(stock_data['Date'].iloc[0], '%Y-%m-%d').date()
        first_date = datetime.datetime.strptime(stock_data['Date'].iloc[-1], '%Y-%m-%d').date()
        start_date = datetime.datetime.strptime(start_date, '%Y-%m-%d').date()
        end_date = datetime.datetime.strptime(end_date, '%Y-%m-%d').date()
        if 'begin' in extra_info:
            end_date = datetime.datetime.strptime(extra_info.split('_')[1], '%Y-%m-%d').date()
        elif 'end' in extra_info:
            start_date = datetime.datetime.strptime(extra_info.split('_')[1], '%Y-%m-%d').date()
        start_date_limit = date_safe_range(start_date, frequency)
        end_date_limit = date_safe_range(end_date, frequency)

    assert (extra_info in ['inRange', 'incorrect'] or
            start_date_limit[0] <= first_date <= start_date_limit[1] or
            start_date_limit[0] < first_date)
    assert extra_info in ['inRange', 'incorrect'] or end_date_limit[0] <= last_date <= end_date_limit[1]

    if not db_save:
        try:
            time.sleep(1)
            os.remove(Path(data_directory, 'test_database.db'))
        except FileNotFoundError:
            print("Database does not exists!")


@pytest.mark.parametrize(
    'symbol, frequency, db_save, extra_info',
    [
        ('TSLA', '1wk', False,  ''),
        (['NKLA', 'SNAP'], '1d', False, ''),
        ('AAPL', '1mo', False, ''),
        ('XYZ', '1d', False, 'incorrect')
    ]
)
@pytest.mark.scraper
@pytest.mark.update
def test_update_data(data_directory, symbol, frequency, db_save, extra_info):
    """Test updating data already exists and new one."""
    current_day: datetime.date = datetime.datetime.now().date()
    start_date_limit: List[datetime.date] = []
    last_date: datetime.date = datetime.datetime.now()

    stock_data: pd.DataFrame = app.update_historical_data(symbol, frequency, save_database=db_save,
                                                          database_name=f'{data_directory}\\test_database.db')

    if stock_data is not None:
        last_date = datetime.datetime.strptime(stock_data['Date'].iloc[0], '%Y-%m-%d').date()
        start_date_limit = date_safe_range(current_day, frequency)

    assert extra_info in ['incorrect'] or start_date_limit[0] <= last_date <= start_date_limit[1]


@pytest.mark.scraper
def test_date_check(data_directory, data):
    """
    Test method responsible for checking data and frequency.
    """
    conn = sqlite3.connect(Path(data_directory, 'test_database.db'))
    database_names: List[str] = ['stock_TSLA|2020-08-01-2023-03-01&freq=1d',
                                 'stock_NVDA|2022-05-01-2023-05-01&freq=1d']
    for table in database_names:
        data.to_sql(table, conn, if_exists='replace', index=False)
    assert app.date_and_freq_check('TSLA', datetime.date(2021, 1, 1),
                                   datetime.date(2023, 1, 1), '1d', conn) is False
    assert (app.date_and_freq_check('NVDA', datetime.date(2021, 1, 1),
                                    datetime.date(2023, 1, 1), '1d', conn)
            == (True, datetime.datetime(2022, 5, 1, 0, 0), 'start'))
    conn.close()


@pytest.mark.scraper
def test_extract_date():
    """Test method for extracting date from the table name."""
    table_name: str = 'stock_TSLA_2020-08-01-2023-03-01&freq=1d'
    table_start, table_end = app.extract_date_from_table(table_name)
    assert table_start == datetime.date(2020, 8, 1)
    assert table_end == datetime.date(2023, 3, 1)


@pytest.mark.csvfile
@pytest.mark.scraper
def test_download_csv_list(data_directory):
    """Test downloading data from the csv file with stock symbols."""
    current_day: datetime.date = datetime.datetime.now().date()
    stock_symbols = pd.read_csv(Path(data_directory, 'test_symbols.csv'), header=None)[0].values
    data: pd.DataFrame = app.download_historical_data(symbols=stock_symbols, start='2023-05-01', end=str(current_day),
                                                      database_name=f'{data_directory}/test_database.db',
                                                      save_database=False)

    companies: np.ndarray = data['Company'].unique()
    result = set(stock_symbols).intersection(companies)
    assert set(stock_symbols) == result
