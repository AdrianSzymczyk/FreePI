import functools
import os
import logging
import sqlite3
import subprocess
import time
import shutil
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import List, Tuple, Union
from pandas import DataFrame

from config import config
from config.config import logger
import numpy as np
import pandas as pd
import re
from backend import technical_indicators

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import selenium.common.exceptions
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager


def setup_webdriver() -> webdriver:
    """
    Create and configure webdriver options and add extensions.
    :return: Webdriver for remote access to browser
    """
    # Setup options for Chrome browser
    chrome_options = webdriver.ChromeOptions()

    # Adding argument to disable the AutomationControlled flag
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")

    # Running in Headless Mode (Do not display browser)
    # chrome_options.add_argument('--headless')

    # Exclude the collection of enable-logging switches
    chrome_options.add_argument("--ignore-certificate-errors")
    chrome_options.add_experimental_option('excludeSwitches', ['enable-logging'])

    # Turn-off userAutomationExtension
    chrome_options.add_experimental_option("useAutomationExtension", False)
    try:
        os.environ['WDM_LOG'] = str(logging.NOTSET)
        chr_driver = webdriver.Chrome(options=chrome_options,
                                      service=Service(ChromeDriverManager().install())
                                      # service=chrome_service
                                      )
    except selenium.common.exceptions.NoSuchDriverException:
        service = webdriver.ChromeService(service_args=['--log-level=OFF', '--disable-build-check'],
                                          log_output=subprocess.STDOUT)
        chr_driver = webdriver.Chrome(service=service, options=chrome_options)
    chr_driver.set_page_load_timeout(10)
    return chr_driver


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
    if len(table.split('_')) > 3:
        table_date = table.split('_')[3].split('&')[0]
    elif len(table.split('_')) > 2:
        table_date = table.split('_')[2].split('&')[0]
    else:
        table_date = table.split('_')[1].split('&')[0]
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

    if connection is None:
        if 'test' in database_name:
            connection = sqlite3.connect(f'{database_name}')
        else:
            connection = sqlite3.connect(f'{Path(config.DATA_DICT, database_name)}')
    find_table_query = (f"SELECT name "
                        f"FROM sqlite_master "
                        f"WHERE type='table' AND name LIKE '%{symbol}%freq={frequency}%';")
    cursor = connection.execute(find_table_query)
    try:
        all_tables = cursor.fetchall()
        table_name = all_tables[0][0]
        return table_name
    except IndexError:
        pass


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
            if database_table_name.split('_')[2] == 'oldest':
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


# TODO: think about the asynchronous symbol handling and saving into the database
def symbol_handler(driver: webdriver, symbol: str, start_date: datetime, end_date: datetime,
                   frequency: str, database_name: str = 'stock_database.db') -> pd.DataFrame | str | None:
    """
    Support method for download_historical_data method and list of symbols
    :param driver: Webdriver for remote control and browsing the webpage
    :param symbol: Stock market symbol
    :param start_date: Beginning of the period of time
    :param end_date: End of the period of time
    :param frequency: String specifying the frequency of the data, defaults-1d, possible values: [1d, 1wk, 1mo]
    :param database_name: Name of the database where data will be saved. Default "stock_database"
    :return: Pandas DataFrame with fetch data from the webpage
    """
    # Upper case symbol
    symbol = symbol.upper()
    use_previous_start_date: bool = False
    previous_start_date: str = ''

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
                return
            except OSError as e:
                print('Error: %s - %s.' % (e.filename, e.strerror))

        # Collect all data from the webpage
        all_data_loaded: bool = False
        while not all_data_loaded:
            # Variables to handle freezing webpage and not scrolling down
            endless_loop: bool = False
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located(
                    (By.XPATH, '//*[@id="Col1-1-HistoricalDataTable-Proxy"]/section/div[2]/table'))
            )

            # Get the initial scroll position
            prev_scroll_position = driver.execute_script("return window.pageYOffset;")
            while True:
                # Scroll down to bottom
                try:
                    driver.execute_script(
                        'window.scrollTo(0, document.getElementById("render-target-default").scrollHeight);')
                except selenium.common.exceptions.StaleElementReferenceException:
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
                        all_data_loaded = True
                        # Change start date into last date from the yahoo finance
                        start_date = 'oldest_' + str(last_date)
                        break

                # Update the previous scroll position for the next iteration
                prev_scroll_position = current_scroll_position
            # Refresh webpage caused by not loading data
            if endless_loop:
                print('Refreshing page!!!')
                driver.refresh()

        # Get all data from the loaded table
        stock_table = driver.find_element(By.XPATH,
                                          '//*[@id="Col1-1-HistoricalDataTable-Proxy"]/section/div[2]/table')
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

        if use_previous_start_date:
            return stock_df, previous_start_date
        else:
            return stock_df, start_date
    else:
        print('Data in a given date range already exists')


def reset_database() -> None:
    """Reset database by deleting it and creating new one"""
    try:
        backup_database()
        os.remove(Path(config.DATA_DICT, 'stock_database.db'))
    except FileNotFoundError:
        print('Database does not exist!')
    conn = sqlite3.connect(f'{Path(config.DATA_DICT, "stock_database.db")}')
    conn.close()


def backup_database() -> None:
    """Create a backup version of the database"""
    current_day = datetime.now().date()
    database_path: Path = Path(config.DATA_DICT, 'stock_database.db')
    backup_path: Path = Path(config.DATA_DICT, 'backups', f'backup_database_{current_day}.db')
    if os.path.isfile(database_path):
        shutil.copy2(database_path, backup_path)


def delete_duplicates(connection: sqlite3.Connection, table_name: str) -> None:
    """
    Delete duplicates from the specified database table
    :param connection: Connection to the SQLite database
    :param table_name: Name of the database table from which to remove duplicates
    """
    duplicate_query: str = f'''
        SELECT Date, COUNT(*)
        FROM `{table_name}`
        GROUP BY Date, Volume
        HAVING COUNT(*) > 1
    '''
    delete_duplicate_query: str = f'''
        DELETE FROM `{table_name}`
        WHERE ROWID NOT IN (
            SELECT MIN(ROWID)
            FROM `{table_name}`
            GROUP BY Date
        );
    '''
    cursor = connection.cursor()
    cursor.execute(delete_duplicate_query)
    connection.commit()


def save_into_database(connection: sqlite3.Connection, data: pd.DataFrame, symbol: str,
                       start_date: Union[datetime.date, str],
                       end_date: datetime.date, frequency: str,
                       database_name: str = 'stock_database.db') -> None:
    """
    Save data to a database specific table
    :param connection: Connection to the SQLite database
    :param data: Pandas DataFrame with stock symbol data
    :param symbol: Stock market symbol
    :param start_date: Beginning of the period of time
    :param end_date: End of the period of time
    :param frequency: String specifying the frequency of the data, defaults-1d, possible values: [1d, 1wk, 1mo]
    :param database_name: Name of the database where data will be saved. Default "stock_database"
    """
    # Create a table name
    table_name = f'stock_{symbol}_{start_date}-{end_date}&freq={frequency}'
    # Define the table schema
    create_table_query = '''
                    CREATE TABLE IF NOT EXISTS master_table (
                        "symbol" TEXT,
                        "table_name" TEXT,
                        PRIMARY KEY("symbol")
                    );
                    '''
    # Execute the query to create the table
    connection.execute(create_table_query)
    insert_query = '''
                    INSERT OR REPLACE INTO master_table (symbol, table_name)
                    VALUES (?, ?);
                    '''
    connection.execute(insert_query, (symbol + '_' + frequency, table_name))

    # Get the name of the stock symbol table
    database_table_name: str = get_name_of_symbol_table(symbol, frequency, connection, database_name)
    if database_table_name is not None:
        table_start, table_end = extract_date_from_table(database_table_name)
        if not isinstance(start_date, str):
            if table_start < start_date:
                if database_table_name.split('_')[2] == 'oldest':
                    table_start = 'oldest_' + str(table_start)
                table_name = f'stock_{symbol}_{table_start}-{end_date}&freq={frequency}'
        # Add Pandas dataframe to the sql database
        data.to_sql(database_table_name, connection, if_exists='append', index=False)
        change_table_name_query = f'ALTER TABLE `{database_table_name}` RENAME TO `{table_name}`'
        cursor = connection.cursor()
        cursor.execute(change_table_name_query)
    else:
        data.to_sql(table_name, connection, if_exists='append', index=False)

    # Check whether duplicates occur inside the table
    delete_duplicates(connection, table_name)


def download_historical_data(symbols: str | List[str] | np.ndarray, start: str, end: str, frequency: str = '1d',
                             save_database: bool = True, database_name: str = 'stock_database.db') -> DataFrame | None:
    """
    Fetch stock market data from the yahoo finance over a given period.
    :param symbols: Stock symbol, accepts a single symbol or a list of symbols
    :param start: Beginning of the period of time, valid format: "2021-09-08"
    :param end: End of the period of time, valid format: "2021-08-08"
    :param frequency: String specifying the frequency of the data, defaults-1d, possible values: [1d, 1wk, 1mo]
    :param save_database: Determine whether to save csv file. Default True
    :param database_name: Name of the database where data will be saved. Default "stock_database"
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
            stock_df, start_to_file = symbol_handler(driver, symbols, start, end, frequency, database_name)
            # Save downloaded data into csv file or return bare DataFrame
            if save_database:
                save_into_database(conn, stock_df, symbols, start_to_file, end_to_file, frequency)
                driver.quit()
                conn.close()
                return stock_df
            else:
                driver.quit()
                conn.close()
                return stock_df
        except TypeError:
            pass
    # Execute downloading for an array of the symbols
    elif isinstance(symbols, list) or isinstance(symbols, np.ndarray):
        all_symbols_df: List[pd.DataFrame] = []
        for symbol in symbols:
            print(f'Download_func - symbol: {symbol}')
            try:
                stock_df, start_to_file = symbol_handler(driver, symbol, start, end, frequency, database_name)
                if save_database:
                    save_into_database(conn, stock_df, symbol, start_to_file, end_to_file, frequency)
                    stock_df['Company'] = symbol
                    all_symbols_df.append(stock_df)
                else:
                    stock_df['Company'] = symbol
                    all_symbols_df.append(stock_df)
            except TypeError:
                pass

        if len(all_symbols_df) != 0:
            # print('\n', pd.concat(all_symbols_df, ignore_index=True))
            driver.quit()
            conn.close()
            return pd.concat(all_symbols_df, ignore_index=True)

    # Quit the webdriver and close the browser and database connection
    driver.quit()
    conn.close()


def display_database_tables() -> None:
    """Display all the database tables"""
    conn = ''
    try:
        conn = sqlite3.connect(f'{Path(config.DATA_DICT, "stock_database.db")}')
        cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
        if tables:
            print('\nTables in the database')
            for table in tables:
                print(table[0])
        else:
            print('No tables in the database')
    except sqlite3.Error as e:
        print(f'Error: {e}')
    finally:
        if not isinstance(conn, str):
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
    return datetime.strptime('1980-01-01', '%Y-%m-%d').date()


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
    updated_data: pd.DataFrame = pd.DataFrame()
    current_date = datetime.now().date()
    if isinstance(symbols, str):
        try:
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

    elif isinstance(symbols, list) or isinstance(symbols, np.ndarray):
        # Variable to determine the start date for update
        latest_end_date: datetime.date = datetime.now().date()
        # Lists of stock symbols to update
        symbols_to_update: list[str] = []
        for symbol in symbols:
            try:
                final_table_end = define_update_range(symbol, frequency, database_name)
            # Except whether wrong frequency was given
            except TypeError:
                # Assign further variables to empty string
                final_table_end = ''
                pass
            try:
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
                                                    frequency, save_database, database_name)
            # Update technical indicators
            if 'test' not in database_name:
                technical_indicators.update_indicators(symbols_to_update, database_name)

    # Create a database backup
    if 'test' not in database_name:
        backup_database()

    return updated_data


def fetch_from_database(symbol: str, frequency: str, connection: sqlite3.Connection | None = None,
                        database_name: str = 'stock_database.db') -> None:
    """
    Fetch and display data from the database symbol table
    :param connection: Connection to the database.
    :param symbol: Stock symbol, accepts a single symbol or a list of symbols
    :param frequency: String specifying the frequency of the data, defaults-1d, possible values: [1d, 1wk, 1mo]
    :param database_name: Name of the database where data will be saved. Default "stock_database"
    """
    if connection is None:
        connection = sqlite3.connect(f'{Path(config.DATA_DICT, "stock_database.db")}')
    try:
        table_name: str = get_name_of_symbol_table(symbol, frequency, connection, database_name)
        sort_query: str = f'SELECT * FROM `{table_name}` ORDER BY Date DESC'
        cursor = connection.execute(sort_query)
        results = cursor.fetchall()
        # Fetch all the table columns
        column_exists = connection.execute(f'PRAGMA table_info(`{table_name}`);')
        table_columns = [col[1] for col in column_exists]
        print('\n', pd.DataFrame(results, columns=table_columns))
    except IndexError:
        print(f'No data for {symbol}')
    connection.close()


if __name__ == '__main__':
    pass
    # Tests for stock_symbols file
    download_historical_data('SNAP', start='2023-01-01', end='2023-08-04', save_database=False)
    # reset_database()
    stock_symbols = pd.read_csv(Path(config.DATA_DICT, 'stock_symbols.csv'), header=None)[0].values
    # download_historical_data(symbols=stock_symbols, start='1980-01-01', end='2023-08-04')
    # update_historical_data(stock_symbols, '1d')
    # display_database_tables()
