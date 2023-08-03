import functools
import os
import sqlite3
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import List, Union, Tuple
from config import config
from config.config import logger
import numpy as np
import pandas as pd
import re

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import selenium.common.exceptions


def setup_webdriver() -> webdriver:
    """
    Create and configure webdriver options and add extensions.
    :return: Webdriver for remote access to browser
    """
    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_extension(Path(config.EXTENSIONS_DICT, 'u_block_extension.crx'))
    chrome_options.add_experimental_option('detach', True)
    chrome_options.add_argument(
        'user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
        'Chrome/115.0.0.0 Safari/537.36')
    # Adding argument to disable the AutomationControlled flag
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")

    # Exclude the collection of enable-automation switches
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])

    # Turn-off userAutomationExtension
    chrome_options.add_experimental_option("useAutomationExtension", False)
    chr_driver = webdriver.Chrome(options=chrome_options)
    chr_driver.set_page_load_timeout(4)
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


def extract_date_from_file(file: str) -> (datetime.date, datetime.date):
    """
    Get the range of the data.
    :param file: Name of the file
    :return: Start and end dates from the file name
    """
    # Check that the data comes from the longest range
    if len(file.split('_')) > 3:
        file_date = file.split('_')[3].split('&')[0]
    elif len(file.split('_')) > 2:
        file_date = file.split('_')[2].split('&')[0]
    else:
        file_date = file.split('_')[1].split('&')[0]
    file_start = datetime.strptime(file_date[:10], '%Y-%m-%d').date()
    file_end = datetime.strptime(file_date[11:], '%Y-%m-%d').date()
    return file_start, file_end


def create_file_list(func):
    @functools.wraps(func)
    def wrapper(symbol, *args, **kwargs):
        # Upper case symbol
        symbol = symbol.upper()
        kwargs['symbol'] = symbol
        # Create variable with a path to the symbol directory
        kwargs['dict_path']: Path = Path(config.DATA_DICT, symbol)
        # Create stock symbol directory if not exists
        if not Path.exists(kwargs['dict_path']):
            os.mkdir(kwargs['dict_path'])
        # Create list with all files inside the directory
        kwargs['all_files'] = [item for item in os.listdir(kwargs['dict_path'])
                               if os.path.isfile(Path(kwargs['dict_path'], item))]
        value = func(*args, **kwargs)
        return value

    return wrapper


def get_name_of_symbol_table(symbol: str, frequency: str, connection: sqlite3.connect = None) -> str:
    """
    Get the name of the stock symbol table from the database
    :param symbol: Stock market symbol
    :param frequency: String specifying the frequency of the data, defaults-1d, possible values: [1d, 1wk, 1mo]
    :param connection: Connection to the SQLite database
    :return: Name of the stock symbol table
    """

    if connection is None:
        connection = sqlite3.connect(f'{Path(config.DATA_DICT, "stock_database.db")}')
    find_table_query = f"SELECT name FROM sqlite_master WHERE type='table' AND name LIKE '%{symbol.lower()}%freq={frequency}%';"
    cursor = connection.execute(find_table_query)
    try:
        all_tables = cursor.fetchall()
        table_name = all_tables[0][0]
        return table_name
    except IndexError:
        print(f'No table for {symbol}')


def date_and_freq_check(symbol: str, input_start_date: datetime, input_end_date: datetime,
                        frequency: str) -> Union[bool, Tuple[bool, datetime.date, str]]:
    """
    Check whether the given date range is covered by existing files.
    :param symbol: Stock market symbol
    :param input_start_date: Beginning of the period of time, valid format: "2021-09-08"
    :param input_end_date: End of the period of time, valid format: "2021-08-08"
    :param frequency: String defining the frequency of the data
    :return: Bool value whether to download new data or tuple with extended information
    """
    # Variable to check if file is with the oldest data
    oldest: bool = False
    try:
        database_table_name: str = get_name_of_symbol_table(symbol, frequency)
        if database_table_name is not None:
            table_start, table_end = extract_date_from_file(database_table_name)
            if database_table_name.split('_')[2] == 'oldest':
                oldest = True
            if table_start <= input_start_date <= table_end:
                if table_start <= input_end_date <= table_end:
                    return False
            if not oldest:
                if input_start_date < table_start:
                    if input_end_date <= table_end:
                        table_start = datetime.strptime(datetime.strftime(table_start, '%Y-%m-%d'), '%Y-%m-%d')
                        return True, table_start, 'start'
            elif input_start_date >= table_start:
                if input_end_date > table_end:
                    table_end = datetime.strptime(datetime.strftime(table_end, '%Y-%m-%d'), '%Y-%m-%d')
                    return True, table_end, 'end'
        else:
            return True
    except IndexError:
        print(f'{symbol} table missing from database')
        return True
    return False


def symbol_handler(driver: webdriver, symbol: str, start_date: datetime,
                   end_date: datetime, frequency: str) -> pd.DataFrame:
    """
    Support method for download_historical_data method and list of symbols
    :param driver: Webdriver for remote control and browsing the webpage
    :param symbol: Stock market symbol
    :param start_date: Beginning of the period of time
    :param end_date: End of the period of time
    :param frequency: String specifying the frequency of the data, defaults-1d, possible values: [1d, 1wk, 1mo]
    :return: Pandas DataFrame with fetch data from the webpage
    """
    # Upper case symbol
    symbol = symbol.upper()
    use_previous_start_date: bool = False
    previous_start_date: str = ''

    result = date_and_freq_check(symbol=symbol, input_start_date=start_date.date(),
                                 input_end_date=end_date.date(), frequency=frequency)
    if isinstance(result, bool):
        condition = result
    else:
        condition, new_time, site_to_change = result
        if site_to_change == 'start':
            # Assign new end_date
            end_date = new_time
        else:
            use_previous_start_date = True
            previous_start_date = start_date.date()
            # Assign new start_date
            start_date = new_time

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
                os.rmdir(Path(config.DATA_DICT, symbol))
                logger.error(f'Incorrect symbol stock "{symbol}", no such stock symbol.')
                return
            except OSError as e:
                print('Error: %s - %s.' % (e.filename, e.strerror))

        # Collect all data from the webpage
        all_data_loaded: bool = False
        while not all_data_loaded:
            # Variables to detect not loading website
            loading_message: str = '#Col1-1-HistoricalDataTable-Proxy > section > div.Pb\(10px\).Ovx\(a\).W\(100\%\) > div'
            tmp_last_date: datetime.date = end_date.date()
            endless_loop: bool = False
            i: int = 0
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located(
                    (By.XPATH, '//*[@id="Col1-1-HistoricalDataTable-Proxy"]/section/div[2]/table'))
            )

            while True:
                driver.execute_script(
                    'window.scrollTo(0, document.getElementById("render-target-default").scrollHeight);')
                time.sleep(0.4)
                try:
                    last_row_date = driver.find_element(By.CSS_SELECTOR,
                                                        '#Col1-1-HistoricalDataTable-Proxy > section > div.Pb\(10px\).Ovx\(a\).W\(100\%\) > table > tbody > tr:last-child > td.Py\(10px\).Ta\(start\)')
                except selenium.common.exceptions.NoSuchElementException:
                    logger.error('No data on the webpage')
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
                try:
                    if lower_start_limit < last_date < upper_start_limit:
                        all_data_loaded = True
                        break
                    elif str(last_date) == str(tmp_last_date) and driver.find_element(By.CSS_SELECTOR, loading_message):
                        break
                except selenium.common.exceptions.NoSuchElementException:
                    if str(last_date) == str(tmp_last_date):
                        print('Reached the end of the data')
                        all_data_loaded = True
                        # Change start date into last date from the yahoo finance
                        start_date = 'oldest_' + str(last_date)
                        break

                # Handle variables responsible for refreshing page when driver gets stuck
                i += 1
                if i > 10:
                    tmp_last_date = last_date

            if endless_loop:
                print('Refreshing page!!!')
                driver.refresh()
        stock_table = driver.find_element(By.XPATH,
                                          '//*[@id="Col1-1-HistoricalDataTable-Proxy"]/section/div[2]/table')

        # Merge downloaded data into arrays and format it
        tmp_arr: np.array = np.array(stock_table.text.split('\n'))
        separated_data = [re.split(r'\s+(?!Close\*\*)', line) for line in tmp_arr[:-1]
                          if 'Dividend' not in line if 'Split' not in line]
        stock_data: List[str] = []
        for i in range(1, len(separated_data)):
            date: str = ' '.join(separated_data[i][:3])
            converted_date: datetime.date = datetime.strftime(datetime.strptime(date, '%b %d, %Y').date(), '%Y-%m-%d')
            stock_data.append([converted_date] + separated_data[i][3:])
        final_list = [separated_data[0]] + stock_data

        # Join created arrays into Pandas DataFrame
        stock_df = pd.DataFrame(final_list[1:], columns=final_list[0])
        if use_previous_start_date:
            return stock_df, previous_start_date
        else:
            return stock_df, start_date
    else:
        print('Data in the given date range already exists')


def reset_database():
    """Reset database by deleting it and creating new one"""
    try:
        os.remove(Path(config.DATA_DICT, 'stock_database.db'))
    except FileNotFoundError:
        print('Database does not exist!')
    conn = sqlite3.connect(f'{Path(config.DATA_DICT, "stock_database.db")}')
    conn.close()


def delete_duplicates(connection: sqlite3.connect, table_name: str) -> None:
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
    # cursor.execute(duplicate_query)
    # duplicates = cursor.fetchall()
    # for duplicate in duplicates:
    #     print(duplicate)
    cursor.execute(delete_duplicate_query)
    connection.commit()


def save_into_database(connection: sqlite3.connect, data: pd.DataFrame, symbol: str, start_date: Union[datetime.date, str],
                       end_date: datetime.date, frequency: str) -> None:
    """
    Save data to a database specific table
    :param connection: Connection to the SQLite database
    :param data: Pandas DataFrame with stock symbol data
    :param symbol: Stock market symbol
    :param start_date: Beginning of the period of time
    :param end_date: End of the period of time
    :param frequency: String specifying the frequency of the data, defaults-1d, possible values: [1d, 1wk, 1mo]
    """
    # save_data(stock_df, symbols, start_to_file, end_to_file, frequency)
    table_name = f'stock_{symbol.lower()}_{start_date}-{end_date}&freq={frequency}'
    # Define the table schema (replace 'your_table_name' and 'column1', 'column2', etc. with your table and column names)
    create_table_query = '''
                    CREATE TABLE IF NOT EXISTS master_table (
                        column1 Stock_symbol,
                        column2 Table_name
                    );
                    '''
    # Execute the query to create the table
    connection.execute(create_table_query)
    insert_query = '''
                    INSERT INTO master_table (column1, column2)
                    VALUES (?, ?);
                    '''
    connection.execute(insert_query, (symbol, table_name))

    # Get the name of the stock symbol table
    database_table_name: str = get_name_of_symbol_table(symbol, frequency, connection)
    if database_table_name is not None:
        table_start, table_end = extract_date_from_file(database_table_name)
        if not isinstance(start_date, str):
            if table_start < start_date:
                if database_table_name.split('_')[2] == 'oldest':
                    table_start = 'oldest_' + str(table_start)
                table_name = f'stock_{symbol.lower()}_{table_start}-{end_date}&freq={frequency}'
        # Add Panda dataframe to the sql database
        data.to_sql(database_table_name, connection, if_exists='append', index=False)
        change_table_name_query = f'ALTER TABLE `{database_table_name}` RENAME TO `{table_name}`'
        cursor = connection.cursor()
        cursor.execute(change_table_name_query)
    else:
        data.to_sql(table_name, connection, if_exists='append', index=False)

    # Check whether duplicates occur inside the table
    delete_duplicates(connection, table_name)


def download_historical_data(symbols: Union[str, List[str], np.ndarray], start: str, end: str, frequency: str = '1d',
                             save_database: bool = True) -> pd.DataFrame:
    """
    Fetch stock market data from the yahoo finance over a given period.
    :param symbols: Stock symbol, accepts a single symbol or a list of symbols
    :param start: Beginning of the period of time, valid format: "2021-09-08"
    :param end: End of the period of time, valid format: "2021-08-08"
    :param frequency: String specifying the frequency of the data, defaults-1d, possible values: [1d, 1wk, 1mo]
    :param save_database: Determine whether to save csv file. Default True
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
    except ValueError as err:
        logger.error(err)
        return

    # Set up the driver and accept cookies
    driver = setup_webdriver()
    # Connect to or create the database file
    conn = sqlite3.connect(f'{Path(config.DATA_DICT, "stock_database.db")}')
    if isinstance(symbols, str):
        try:
            # Create DataFrame with downloaded data from webpage
            stock_df, start_to_file = symbol_handler(driver, symbols, start, end, frequency)
            # Save downloaded data into csv file or return bare DataFrame
            if save_database:
                save_into_database(conn, stock_df, symbols, start_to_file, end_to_file, frequency)
            else:
                driver.quit()
                conn.close()
                return stock_df
            # # Review all files inside symbol directory
            # delete_files_with_less_data_range(symbols)
        except TypeError:
            pass
    elif isinstance(symbols, list) or isinstance(symbols, np.ndarray):
        all_symbols_df: list = []
        for symbol in symbols:
            print(f'Download_func - symbol: {symbol}')
            try:
                stock_df, start_to_file = symbol_handler(driver, symbol, start, end, frequency)
                if save_database:
                    save_into_database(conn, stock_df, symbol, start_to_file, end_to_file, frequency)
                else:
                    stock_df['Company'] = symbol
                    all_symbols_df.append(stock_df)
            except TypeError:
                pass

        if len(all_symbols_df) != 0:
            # print('\n', pd.concat(all_symbols_df, ignore_index=True))
            if not save_database:
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


def define_update_range(symbol: str, frequency: str) -> datetime.date:
    """
    Determine new date range to download new data
    :param symbol: Stock market symbol
    :param frequency: String specifying the frequency of the data, possible values: [1d, 1wk, 1mo]
    :return: Start date of update date range
    """
    database_table_name: str = get_name_of_symbol_table(symbol, frequency)
    if database_table_name is not None:
        table_start, table_end = extract_date_from_file(database_table_name)
        return table_end
    return datetime.strptime('1980-01-01', '%Y-%m-%d').date()


def update_historical_data(symbols: Union[str, List[str], np.ndarray], frequency: str) -> None:
    """
    Update files with latest stock market data
    :param symbols: Stock symbol, accepts a single symbol or a list of symbols
    :param frequency: String specifying the frequency of the data, possible values: [1d, 1wk, 1mo]
    """
    current_date = datetime.now().date()
    if isinstance(symbols, str):
        try:
            final_table_end = define_update_range(symbols, frequency)
        # Except whether wrong frequency was given
        except TypeError:
            return
        if current_date == final_table_end:
            print('Nothing to update, table is up-to-date')
        else:
            # Download data from the new date range and save into symbol table
            download_historical_data(symbols, final_table_end.strftime('%Y-%m-%d'), current_date.strftime('%Y-%m-%d'),
                                     frequency=frequency)

    elif isinstance(symbols, list) or isinstance(symbols, np.ndarray):
        # Variable to determine the start date for update
        latest_end_date: datetime.date = datetime.now().date()
        # Lists of stock symbols to update
        symbols_to_update: list[str] = []
        for symbol in symbols:
            try:
                final_table_end = define_update_range(symbol, frequency)
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
            download_historical_data(symbols_to_update, latest_end_date.strftime('%Y-%m-%d'),
                                     current_date.strftime('%Y-%m-%d'), frequency=frequency)


def fetch_from_database(symbol, frequency) -> None:
    """
    Fetch and display data from the database symbol table
    :param symbol: Stock symbol, accepts a single symbol or a list of symbols
    :param frequency: String specifying the frequency of the data, defaults-1d, possible values: [1d, 1wk, 1mo]
    """
    conn = sqlite3.connect(f'{Path(config.DATA_DICT, "stock_database.db")}')
    try:
        table_name = get_name_of_symbol_table(symbol, frequency, conn)
        sort_query = f'SELECT * FROM `{table_name}` ORDER BY Date DESC'
        cursor = conn.execute(sort_query)
        results = cursor.fetchall()
        columns = ['Date', 'Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume']
        print('\n', pd.DataFrame(results, columns=columns))
    except IndexError:
        print(f'No data for {symbol}')
    conn.close()


if __name__ == '__main__':
    pass

    # Database tests
    # reset_database()
    # download_historical_data(['TSLA', 'NVDA'], start='2022-10-01', end='2023-01-01', frequency='1d')
    # download_historical_data(['TSLA', 'NVDA'], start='2021-12-20', end='2023-01-01', frequency='1d')
    # download_historical_data(['TSLA', 'NVDA'], start='2021-12-20', end='2023-01-07', frequency='1d')
    # download_historical_data(['TSLA'], start='2009-12-20', end='2023-08-02', frequency='1d')
    # download_historical_data(['TSLA'], start='2009-12-20', end='2023-08-02', frequency='1mo')
    # fetch_from_database('TSLA', '1d')

    # update_historical_data('TSLA', '1d')
    # update_historical_data('TSLA', '1mo')
    update_historical_data(['TSLA', 'NVDA'], '1d')
    display_database_tables()

    # conn = sqlite3.connect(f'{Path(config.DATA_DICT, "stock_database.db")}')
    # database_table_name = get_name_of_symbol_table('TSLA', '1d', conn)
    # delete_duplicates(conn, database_table_name)
    # conn.close()
