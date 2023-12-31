import functools
import os
import logging
import sqlite3
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import List, Tuple
from pandas import DataFrame
import numpy as np
import pandas as pd
from config import config
from config.config import logger
import re
from backend import technical_indicators
from webScrape import db_controller

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import selenium.common.exceptions
from selenium.webdriver.remote.webelement import WebElement
import chromedriver_autoinstaller


def setup_webdriver() -> webdriver:
    """
    Create and configure webdriver options and add extensions.
    :return: Webdriver for remote access to browser
    """

    chromedriver_autoinstaller.install()    # Check if the current version of chromedriver exists
                                            # and if it doesn't exist, download it automatically,
                                            # then add chromedriver to path
    # Setup options for Chrome browser
    chrome_options = webdriver.ChromeOptions()

    # Adding argument to disable the AutomationControlled flag
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")

    # Exclude the collection of enable-logging switches
    chrome_options.add_argument('--ignore-ssl-errors=yes')
    chrome_options.add_argument('--ignore-certificate-errors')
    chrome_options.add_experimental_option('excludeSwitches', ['enable-logging'])

    chrome_options.add_argument('log-level=3')
    chrome_options.add_argument('--headless')  # Running in Headless Mode (Do not display browser)
    chrome_options.add_argument("--start-maximized")  # Open Browser in maximized mode
    chrome_options.add_argument("--disable-extensions")  # Disabling extensions
    chrome_options.add_argument('--disable-gpu')  # Applicable to windows as only
    chrome_options.add_argument("--disable-dev-shm-usage")  # Overcome limited resource problems
    chrome_options.add_argument('--no-sandbox')  # Bypass OS security model WebDriver

    # Turn-off userAutomationExtension
    chrome_options.add_experimental_option("useAutomationExtension", False)
    try:
        os.environ['WDM_LOG'] = str(logging.NOTSET)
        driver = webdriver.Chrome(options=chrome_options)
    except selenium.common.exceptions.NoSuchDriverException:
        driver = webdriver.Chrome()
    driver.set_page_load_timeout(10)
    return driver


def initial_driver_run(driver: webdriver,
                       cookie_btn_path: str = '//*[@id="consent-page"]/div/div/div/form/div[2]/div[2]/button[1]'):
    """
    Accept cookies when scraper first launches.
    :param driver: Webdriver for remote control and browsing the webpage
    :param cookie_btn_path: String defining XPath to consent button, defaults: "//*[@id="consent-page"]/div/div/div/form/div[2]/div[2]/button[1]"
    """
    try:
        driver.find_element(By.XPATH, cookie_btn_path).click()
    except selenium.common.exceptions.NoSuchElementException:
        # Pass the method when there is no cookie button on the webpage
        pass
    except selenium.common.exceptions.TimeoutException:
        driver.refresh()


def timer(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.perf_counter()
        result = func(*args, **kwargs)
        end_time = time.perf_counter()
        run_time = end_time - start_time
        print(f'Finished {func.__name__!r} in {run_time:.3f} sec.')
        return result

    return wrapper


def extract_date_from_table(table: str) -> (datetime.date, datetime.date):
    """
    Get the range of the data.
    :param table: Name of the table
    :return: Start and end dates from the table name
    """
    # Check that the data comes from the longest range
    # if len(table.split('_')) > 3:
    #     table_date = table.split('_')[3].split('&')[0]
    if len(table.split('_')) > 2:
        table_date = table.split('_')[2].split('&')[0]
    else:
        table_date = table.split('|')[1].split('&')[0]
    table_start = datetime.strptime(table_date[:10], '%Y-%m-%d').date()
    table_end = datetime.strptime(table_date[11:], '%Y-%m-%d').date()
    return table_start, table_end


def get_name_of_symbol_table(symbol: str, frequency: str, connection: None | sqlite3.Connection = None,
                             database_name: str = 'stock_database.db') -> str:
    """
    Get the name of the stock symbol table from the database
    :param symbol: Stock market symbol
    :param frequency: String specifying the frequency of the data, defaults-1d, possible values: [1d, 1wk, 1mo]
    :param connection: Connection to the SQLite database
    :param database_name: Name of the database where data will be saved. Default "stock_database"
    :return: Name of the stock symbol table
    """
    new_connection: bool = False
    if connection is None:
        new_connection = True
        if 'test' in database_name:
            connection = sqlite3.connect(f'{database_name}')
        else:
            connection = sqlite3.connect(f'{Path(config.DATA_DICT, database_name)}')
    find_table_query = (f"SELECT name "
                        f"FROM sqlite_master "
                        f"WHERE type='table' AND name LIKE 'stock_{symbol}|%freq={frequency}%';")
    cursor = connection.execute(find_table_query)
    try:
        all_tables = cursor.fetchall()
        table_name = all_tables[0][0]
        if new_connection:
            connection.close()
        return table_name
    except IndexError:
        if new_connection:
            connection.close()


def date_and_freq_check(symbol: str, input_start_date: datetime.date, input_end_date: datetime.date,
                        frequency: str, connection: sqlite3.Connection | None = None,
                        database_name: str = 'stock_database.db') \
        -> bool | Tuple[bool, datetime.date, str]:
    """
    Check whether the given date range is covered by existing files.
    :param symbol: Stock market symbol
    :param input_start_date: Beginning of the period of time, valid format: "2021-09-08"
    :param input_end_date: End of the period of time, valid format: "2021-08-08"
    :param frequency: String defining the frequency of the data
    :param connection: Connection to the database
    :param database_name: Name of the database where data will be saved. Default "stock_database"
    :return: Bool value whether to download new data or tuple with extended information
    """
    # Variable to check if file is with the oldest data
    oldest: bool = False
    try:
        database_table_name: str = get_name_of_symbol_table(symbol, frequency, connection, database_name)
        if database_table_name is not None:
            table_start, table_end = extract_date_from_table(database_table_name)
            if database_table_name.split('_')[1].split('|')[1] == 'oldest':
                oldest = True
            if table_start <= input_start_date and input_end_date <= table_end:
                return False
            if not oldest:
                if input_start_date < table_start and input_end_date <= table_end:
                    table_start = datetime(table_start.year, table_start.month, table_start.day)
                    return True, table_start, 'start'
                elif input_start_date >= table_start and input_end_date > table_end:
                    table_end = datetime(table_end.year, table_end.month, table_end.day)
                    return True, table_end, 'end'
            else:
                if input_end_date > table_end:
                    table_end = datetime(table_end.year, table_end.month, table_end.day)
                    return True, (table_start, table_end), 'oldest'
                return False
        return True
    except IndexError:
        print(f'{symbol} table missing from database')
        return True


def symbol_handler(driver: webdriver, symbol: str, start_date: datetime, end_date: datetime,
                   frequency: str, database_name: str = 'stock_database.db',
                   incorrect_symbols: List[str] = None) -> pd.DataFrame | str | None:
    """
    Support method for download_historical_data method and list of symbols
    :param driver: Webdriver for remote control and browsing the webpage
    :param symbol: Stock market symbol
    :param start_date: Beginning of the period of time
    :param end_date: End of the period of time
    :param frequency: String specifying the frequency of the data, defaults-1d, possible values: [1d, 1wk, 1mo]
    :param database_name: Name of the database where data will be saved. Default "stock_database"
    :param incorrect_symbols: Array with incorrect symbols.
    :return: Pandas DataFrame with fetch data from the webpage
    """
    # Upper case symbol
    symbol = symbol.upper()
    use_previous_start_date: bool = False
    previous_start_date: str = ''
    stock_table: selenium.webdriver.remote.webelement = None

    result = date_and_freq_check(symbol, start_date.date(), end_date.date(), frequency, database_name=database_name)
    if isinstance(result, bool):
        condition = result
    else:
        condition, new_time, site_to_change = result
        if site_to_change == 'start':
            # Assign new end_date
            end_date = new_time
        elif site_to_change == 'end':
            use_previous_start_date = True
            previous_start_date = start_date.date()
            # Assign new start_date
            start_date = new_time
        else:
            use_previous_start_date = True
            previous_start_date = f'oldest_{new_time[0]}'
            # Assign new start_date
            start_date = new_time[1]

    if condition:
        # Convert time strings to timestamp format and
        start_time: int = int(start_date.replace(tzinfo=timezone.utc).timestamp())
        end_time: int = int(end_date.replace(tzinfo=timezone.utc).timestamp())
        start_date = start_date.date()
        historical_url = f'https://finance.yahoo.com/quote/{symbol}/history?period1={start_time}&period2={end_time}' \
                         f'&interval={frequency}&filter=history&frequency={frequency}&includeAdjustedClose=true'
        try:
            driver.get(historical_url)
            initial_driver_run(driver)
        except selenium.common.exceptions.TimeoutException:
            print('Timed out receiving message')
            driver.refresh()

        # Check whether stock symbol exists
        current_url: str = driver.current_url
        if f'&frequency={frequency}' not in current_url or driver.title == "Requested symbol wasn't found":
            try:
                logger.error(f'Incorrect symbol stock "{symbol}", no such stock symbol.')
                incorrect_symbols.append(symbol)
                return
            except OSError as e:
                print('Error: %s - %s.' % (e.filename, e.strerror))

        # Collect all data from the webpage
        all_data_loaded: bool = False
        while not all_data_loaded:
            # Variables to handle freezing webpage and not scrolling down
            endless_loop: bool = False
            try:
                WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located(
                        (By.XPATH, '//*[@id="Col1-1-HistoricalDataTable-Proxy"]/section/div[2]/table'))
                )
            except selenium.common.TimeoutException:
                driver.refresh()
            # Get the initial scroll position
            prev_scroll_position = driver.execute_script("return window.pageYOffset;")
            while True:
                # Scroll down to bottom
                try:
                    driver.execute_script(
                        'window.scrollTo(0, document.getElementById("render-target-default").scrollHeight);')
                except selenium.common.exceptions.StaleElementReferenceException:
                    driver.refresh()
                except selenium.common.exceptions.JavascriptException:
                    driver.refresh()
                # Wait to load page
                time.sleep(0.2)
                # Get the current scroll position
                current_scroll_position = driver.execute_script("return window.pageYOffset;")
                try:
                    last_row_date = driver.find_element(By.CSS_SELECTOR,
                                                        '#Col1-1-HistoricalDataTable-Proxy > section > div.Pb\(10px\).Ovx\(a\).W\(100\%\) > table > tbody > tr:last-child > td.Py\(10px\).Ta\(start\)')
                except selenium.common.exceptions.NoSuchElementException:
                    all_data_loaded = True
                    break
                except selenium.common.exceptions.TimeoutException:
                    driver.refresh()
                    last_row_date = ''
                try:
                    last_date: datetime.date = datetime.strptime(last_row_date.text, "%b %d, %Y").date()
                except AttributeError:
                    last_date = datetime.now().date()
                # Adjust lower and upper limits of last displayed date
                if frequency == '1wk':
                    lower_start_limit: datetime.date = start_date - timedelta(days=7)
                    upper_start_limit: datetime.date = start_date + timedelta(days=7)
                elif frequency == '1mo':
                    lower_start_limit: datetime.date = start_date - timedelta(days=31)
                    upper_start_limit: datetime.date = start_date + timedelta(days=31)
                else:
                    lower_start_limit: datetime.date = start_date - timedelta(days=4)
                    upper_start_limit: datetime.date = start_date + timedelta(days=4)

                # Check whether all the data loaded
                if lower_start_limit < last_date < upper_start_limit:
                    if last_date.year == 1972:
                        print('1972-06-02 reached DEAD END')
                        start_date = 'oldest_' + str(last_date)

                    # Get all data from the loaded table
                    try:
                        stock_table = driver.find_element(By.XPATH,
                                                          '//*[@id="Col1-1-HistoricalDataTable-Proxy"]/section/div[2]/table')
                    except selenium.common.exceptions.NoSuchElementException:
                        driver.refresh()

                    all_data_loaded = True
                    break

                # If the current scroll position is the same as the previous position, you've reached the end
                if current_scroll_position == prev_scroll_position:
                    try:
                        driver.find_element(By.XPATH, '//*[@id="Col1-1-HistoricalDataTable-Proxy"]/section/div[2]/div')
                        endless_loop = True
                        break
                    except selenium.common.exceptions.NoSuchElementException:
                        print('Reached the end of the data')
                        # Change start date into last date from the yahoo finance
                        start_date = 'oldest_' + str(last_date)

                        # Get all data from the loaded table
                        try:
                            stock_table = driver.find_element(By.XPATH,
                                                              '//*[@id="Col1-1-HistoricalDataTable-Proxy"]/section/div[2]/table')
                        except selenium.common.exceptions.NoSuchElementException:
                            driver.refresh()

                        all_data_loaded = True
                        break

                # Update the previous scroll position for the next iteration
                prev_scroll_position = current_scroll_position
            # Refresh webpage caused by not loading data
            if endless_loop:
                driver.refresh()

        if use_previous_start_date:
            return stock_table, previous_start_date
        else:
            return stock_table, start_date
    else:
        print('Data in a given date range already exists')


def data_converter(stock_table: WebElement) -> pd.DataFrame:
    """
    Convert data into Pandas DataFrame fetch from the webpage.
    :param stock_table: Selenium webElement with the data fetched from the database.
    :return: Pandas DataFrame with stock symbol data.
    """
    # Merge downloaded data into arrays and format it
    tmp_arr: np.array = np.array(stock_table.text.split('\n'))
    separated_data = [re.split(r'\s+(?!Close\*\*)', line) for line in tmp_arr[:-1]
                      if 'Dividend' not in line if 'Split' not in line]
    # Remove stars from column names
    new_column_list: List[str] = []
    for column_name in separated_data[0]:
        if '*' in column_name:
            new_column_list.append(column_name.replace('*', ''))
        else:
            new_column_list.append(column_name)
    separated_data[0] = new_column_list
    # Concatenate date elements into one element
    stock_data: List = []
    for i in range(1, len(separated_data)):
        date: str = ' '.join(separated_data[i][:3])
        converted_date: datetime.date = datetime.strftime(datetime.strptime(date, '%b %d, %Y').date(), '%Y-%m-%d')
        stock_data.append([converted_date] + separated_data[i][3:])
    final_list = [separated_data[0]] + stock_data

    # Join created arrays into Pandas DataFrame
    stock_df = pd.DataFrame(final_list[1:], columns=final_list[0])

    # Convert numeric columns to appropriate data type
    numeric_columns: List[str] = ['Open', 'High', 'Low', 'Close', 'Adj Close']
    # Remove '-' values with 0
    try:
        stock_df[numeric_columns] = stock_df[numeric_columns].astype(float)
    except ValueError:
        stock_df = stock_df.replace('-', '0')
        for column in numeric_columns:
            stock_df[column] = stock_df[column].str.replace(',', '')
        stock_df[numeric_columns] = stock_df[numeric_columns].astype(float)

    # Remove commas from "Volume" column and convert to integer
    try:
        stock_df['Volume'] = stock_df['Volume'].str.replace(',', '').astype(np.int64)
    except ValueError:
        stock_df['Volume'] = stock_df['Volume'].replace('-', '0')
        stock_df['Volume'] = stock_df['Volume'].str.replace(',', '').astype(np.int64)

    return stock_df


def download_historical_data(symbols: str | List[str] | np.ndarray, start: str, end: str, frequency: str = '1d',
                             save_database: bool = True, database_name: str = 'stock_database.db',
                             update_list: List[str] = None) -> DataFrame | None:
    """
    Fetch stock market data from the yahoo finance over a given period.
    :param symbols: Stock symbol, accepts a single symbol or a list of symbols
    :param start: Beginning of the period of time, valid format: "2021-09-08"
    :param end: End of the period of time, valid format: "2021-08-08"
    :param frequency: String specifying the frequency of the data, defaults-1d, possible values: [1d, 1wk, 1mo]
    :param save_database: Determine whether to save csv file. Default True
    :param database_name: Name of the database where data will be saved. Default "stock_database"
    :param update_list: Array with symbols to update.
    """
    # Check if given frequency is in correct format
    if frequency not in ['1d', '1wk', '1mo']:
        logger.info('Wrong frequency given')
        return
    # Validate given dates
    try:
        # Convert start and end time into datetime format
        start = datetime.strptime(start, '%Y-%m-%d')
        start_to_file = start.date()
        end = datetime.strptime(end, '%Y-%m-%d')
        end_to_file = end.date()
        if start_to_file > end_to_file:
            logger.info('Start date is greater than end date')
            return
        # Change end date to actual date if given date is out of range
        if end_to_file > datetime.now().date():
            end_to_file = datetime.now().date()
            end = datetime.strptime(str(datetime.now().date()), '%Y-%m-%d')
    except ValueError as err:
        logger.error(err)
        return

    # Set up the driver and accept cookies
    driver = setup_webdriver()
    # Connect to or create the database file
    if 'test' in database_name:
        conn = sqlite3.connect(f'{database_name}')
    else:
        conn = sqlite3.connect(f'{Path(config.DATA_DICT, database_name)}')
    # Execute downloading for single symbol
    if isinstance(symbols, str):
        try:
            # Create DataFrame with downloaded data from webpage
            stock_table, start_to_file = symbol_handler(driver, symbols, start, end, frequency, database_name, [])
            if stock_table is not None:
                stock_df = data_converter(stock_table)
                # Save downloaded data into csv file or return bare DataFrame
                if save_database:
                    db_controller.save_into_database(conn, stock_df, symbols, start_to_file, end_to_file, frequency)
                if 'test' in database_name:
                    driver.quit()
                    conn.close()
                    return stock_df
        except TypeError:
            pass

    # Execute downloading for list of symbols
    elif isinstance(symbols, list) or isinstance(symbols, np.ndarray):
        all_symbols_df: List[pd.DataFrame] = []
        incorrect_symbols: List[str] = []
        for symbol in symbols:
            print(f'Download_func - symbol: {symbol}')
            # Download data for single symbol
            try:
                stock_df, start_to_file = symbol_handler(driver, symbol, start, end, frequency, database_name,
                                                         incorrect_symbols)
                if stock_df is None:
                    continue
                stock_df = data_converter(stock_df)
                if save_database:
                    # Save data to the database and append to the shared DataFrame
                    db_controller.save_into_database(conn, stock_df, symbol, start_to_file, end_to_file, frequency)
                    stock_df['Company'] = symbol
                    all_symbols_df.append(stock_df)
                else:
                    # Append data to the shared DataFrame
                    stock_df['Company'] = symbol
                    all_symbols_df.append(stock_df)
            except TypeError:
                pass

        # Remove incorrect symbols from the symbols list to be updated
        for symbol in incorrect_symbols:
            update_list.remove(symbol)

        if len(all_symbols_df) != 0 and 'test' in database_name:
            driver.quit()
            conn.close()
            return pd.concat(all_symbols_df, ignore_index=True)

    # Quit the webdriver and close the browser and database connection
    driver.quit()
    conn.close()


def define_update_range(symbol: str, frequency: str, database_name: str = 'stock_database.db') -> datetime.date:
    """
    Determine new date range to download new data
    :param symbol: Stock market symbol
    :param frequency: String specifying the frequency of the data, possible values: [1d, 1wk, 1mo]
    :param database_name: Name of the database where data will be saved. Default "stock_database"
    :return: Start date of update date range
    """
    database_table_name: str = get_name_of_symbol_table(symbol, frequency, database_name=database_name)
    if database_table_name is not None:
        table_start, table_end = extract_date_from_table(database_table_name)
        return table_end
    return datetime.strptime('1972-06-02', '%Y-%m-%d').date()


@timer
def update_historical_data(symbols: str | List[str] | np.ndarray, frequency: str, save_database: bool = True,
                           database_name: str = 'stock_database.db') -> pd.DataFrame | None:
    """
    Update files with latest stock market data
    :param symbols: Stock symbol, accepts a single symbol or a list of symbols
    :param frequency: String specifying the frequency of the data, possible values: [1d, 1wk, 1mo]
    :param save_database: Determine whether to save csv file. Default True
    :param database_name: Name of the database where data will be saved. Default "stock_database"
    """
    # Create empty variables for restoring the data
    updated_data: pd.DataFrame = pd.DataFrame()
    current_date = datetime.now().date()
    # Update data for single symbol
    if isinstance(symbols, str):
        try:
            # Define start date of the range
            final_table_end = define_update_range(symbols, frequency, database_name)
        # Except whether wrong frequency was given
        except TypeError:
            return
        if current_date == final_table_end:
            print('Nothing to update, table is up-to-date')
        else:
            # Download data from the new date range and save into symbol table
            updated_data = download_historical_data(symbols, final_table_end.strftime('%Y-%m-%d'),
                                                    current_date.strftime('%Y-%m-%d'),
                                                    frequency, save_database, database_name)
            # Update technical indicators
            if 'test' not in database_name:
                technical_indicators.update_indicators(symbols, database_name)
    # Update data for list of symbols
    elif isinstance(symbols, List) or isinstance(symbols, np.ndarray):
        # Variable to determine the start date for update
        latest_end_date: datetime.date = datetime.now().date()
        # Lists of stock symbols to update
        symbols_to_update: List[str] = []
        for symbol in symbols:
            try:
                final_table_end = define_update_range(symbol, frequency, database_name)
            # Except whether wrong frequency was given
            except TypeError:
                # Assign further variables to empty string
                final_table_end = ''
                pass
            try:
                # Append symbol to symbols_to_update
                if final_table_end < current_date:
                    symbols_to_update.append(symbol)
                if final_table_end < latest_end_date:
                    latest_end_date = final_table_end
            except TypeError:
                pass

        if current_date == latest_end_date:
            print('Nothing to update, table is up-to-date')
        else:
            # Download data from the new date range and save into symbol table
            updated_data = download_historical_data(symbols_to_update, latest_end_date.strftime('%Y-%m-%d'),
                                                    current_date.strftime('%Y-%m-%d'),
                                                    frequency, save_database, database_name, symbols_to_update)
            # Update technical indicators
            if 'test' not in database_name or save_database:
                technical_indicators.update_indicators(symbols_to_update, database_name)

    # Create a database backup or return pandas DataFrame with data
    if 'test' not in database_name:
        db_controller.backup_database()
    else:
        return updated_data


# TODO: Run lambda function in AWS at night when stock market is closed and new day starts,
#  ex. for 27.11.2023 script should be evoked at 28.11.2023


if __name__ == '__main__':
    pass
    # Tests for stock_symbols file
    # download_historical_data('SNAP', start='1980-01-01', end='2023-08-04')
    # reset_database()
    stock_symbols = pd.read_csv(Path(config.DATA_DICT, 'stock_symbols.csv'), header=None)[0].values
    # download_historical_data(symbols=stock_symbols, start='1980-01-01', end='2023-08-04')
    update_historical_data(stock_symbols, '1d')
    # db_controller.display_database_tables()
