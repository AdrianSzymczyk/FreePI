import functools
import os
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import List, Union
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


def extract_date_from_file(file: str) -> (datetime.date, datetime.date):
    """
    Get the range of the data.
    :param file: Name of the file
    :return: Start and end dates from the file name
    """
    # Check that the data comes from the longest range
    if len(file.split('_')) > 2:
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


@create_file_list
def date_and_freq_check(input_start_date: datetime, input_end_date: datetime, frequency: str,
                        update: bool = False, **kwargs) -> bool:
    """
    Check whether the given date range is covered by existing files.
    :param input_start_date: Beginning of the period of time, valid format: "2021-09-08"
    :param input_end_date: End of the period of time, valid format: "2021-08-08"
    :param frequency: String defining the frequency of the data
    :param update: Determine whether update method is called
    :return: Bool value whether to download new data
    """
    # Variable to check if file is with the oldest data
    oldest: bool = False
    for file in kwargs['all_files']:
        file_freq: str = file.split('=')[1].split('.')[0]
        if file_freq == frequency:
            file_start, file_end = extract_date_from_file(file)
            if file.split('_')[1] == 'oldest':
                oldest = True
            elif file_start <= input_start_date <= file_end:
                if file_start <= input_end_date <= file_end:
                    return True
        # Check if the file contains the oldest data from the yahoo finance
        if oldest:
            if update:
                return False
            else:
                return True
    return False


def save_data(data: pd.DataFrame, symbol: str, start_date: Union[datetime.date, str], end_date: datetime.date,
              frequency: str):
    """
    Save data in csv file and create sub folder whether not exists
    :param data: Pandas DataFrame with data to save
    :param symbol: Stock symbol
    :param start_date: Beginning of the period of time
    :param end_date: End of the period of time
    :param frequency: String specifying the frequency of the data
    :return:
    """
    if data is None:
        print('Invalid data format!')
    else:
        # Create sub folder for stock symbol whether it doesn't exist
        os.makedirs(Path(config.DATA_DICT, symbol), exist_ok=True)
        # Save downloaded data into csv file
        file_name: str = f'{symbol}_{start_date}-{end_date}&freq={frequency}.csv'
        data.to_csv(Path(config.DATA_DICT, symbol, file_name), index_label=False, index=False)


@create_file_list
def delete_files_with_less_data_range(**kwargs) -> None:
    """Review all the files and decide whether to delete any of them."""
    try:
        # Loop over the list with files names
        for file in kwargs['all_files']:
            # Create an empty list of files to be deleted
            delete_list: List[str] = []
            # Create a variables with a date range
            file_start, file_end = extract_date_from_file(file)
            # Create a variables with frequency of the data and information if file is oldest
            frequency: str = file.split('=')[1].split('.')[0]
            file_oldest: str = file.split('_')[1]
            for inside_file in kwargs['all_files']:
                if file == inside_file:
                    pass
                else:
                    # Variables for inside file
                    inside_file_start, inside_file_end = extract_date_from_file(inside_file)
                    inside_freq: str = inside_file.split('=')[1].split('.')[0]
                    inside_oldest: str = inside_file.split('_')[1]
                    # Remove file from the directory if data already exists
                    if file_start <= inside_file_start and file_end >= inside_file_end and frequency == inside_freq:
                        print(f'1. Removed file: {inside_file}')
                        delete_list.append(inside_file)
                        os.remove(Path(kwargs['dict_path'], inside_file))
                    elif file_start <= inside_file_start and file_end >= inside_file_end and frequency == inside_freq \
                            and file_oldest == inside_oldest:
                        print(f'2. Removed file: {inside_file}')
                        delete_list.append(inside_file)
                        os.remove(Path(kwargs['dict_path'], inside_file))
            for elem in delete_list:
                kwargs['all_files'].remove(elem)
    # Raise exception if the directory for the symbol does not exist
    except FileNotFoundError:
        print(f'Directory for "{kwargs["symbol"]}" was not found')


def symbol_handler(driver: webdriver, symbol: str, start_date: datetime, end_date: datetime,
                   frequency: str, update: bool = False) -> pd.DataFrame:
    """
    Support method for download_historical_data method and list of symbols
    :param driver: Webdriver for remote control and browsing the webpage
    :param symbol: Stock market symbol
    :param start_date: Beginning of the period of time, valid format: "2021-09-08"
    :param end_date: End of the period of time, valid format: "2021-09-08"
    :param frequency: String specifying the frequency of the data, defaults-1d, possible values: [1d, 1wk, 1mo]
    :param update: Determine whether update method is called
    :return: Pandas DataFrame with fetch data from the webpage
    """
    # Upper case symbol
    symbol = symbol.upper()
    if not date_and_freq_check(symbol=symbol, input_start_date=start_date.date(), input_end_date=end_date.date(),
                               frequency=frequency, update=update):
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
                time.sleep(0.5)
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
                        endless_loop = True
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
            date = ' '.join(separated_data[i][:3])
            stock_data.append([date] + separated_data[i][3:])
        final_list = [separated_data[0]] + stock_data

        # Join created arrays into Pandas DataFrame
        stock_df = pd.DataFrame(final_list[1:], columns=final_list[0])
        return stock_df, start_date
    else:
        print('Data in the given date range already exists')


def download_historical_data(symbols: Union[str, List[str], np.ndarray], start: str, end: str, frequency: str = '1d',
                             save_csv: bool = True, update: bool = False) -> pd.DataFrame:
    """
    Fetch stock market data from the yahoo finance over a given period.
    :param symbols: Stock symbol, accepts a single symbol or a list of symbols
    :param start: Beginning of the period of time, valid format: "2021-09-08"
    :param end: End of the period of time, valid format: "2021-08-08"
    :param frequency: String specifying the frequency of the data, defaults-1d, possible values: [1d, 1wk, 1mo]
    :param save_csv: Determine whether to save csv file. Default True
    :param update: Determine whether update method is called
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
    if isinstance(symbols, str):
        try:
            # Create DataFrame with downloaded data from webpage
            stock_df, start_to_file = symbol_handler(driver, symbols, start, end, frequency, update)
            # Save downloaded data into csv file or return bare DataFrame
            if save_csv:
                save_data(stock_df, symbols, start_to_file, end_to_file, frequency)
            else:
                driver.quit()
                return stock_df
            # Review all files inside symbol directory
            delete_files_with_less_data_range(symbols)
        except TypeError:
            pass
    elif isinstance(symbols, list) or isinstance(symbols, np.ndarray):
        all_symbols_df: list = []
        for symbol in symbols:
            print(f'Download_func - symbol: {symbol}')
            try:
                stock_df, start_to_file = symbol_handler(driver, symbol, start, end, frequency, update)
                if save_csv:
                    save_data(stock_df, symbol, start_to_file, end_to_file, frequency)
                else:
                    stock_df['Company'] = symbol
                    all_symbols_df.append(stock_df)
                # Review all files inside symbol directory
                delete_files_with_less_data_range(symbol)
            except TypeError:
                pass

        if len(all_symbols_df) != 0:
            # print('\n', pd.concat(all_symbols_df, ignore_index=True))
            if not save_csv:
                driver.quit()
                return pd.concat(all_symbols_df, ignore_index=True)

    # Quit the webdriver and close the browser
    driver.quit()


def file_latest_data_checker(symbol: str, file_name: str, new_data: pd.DataFrame) -> pd.DataFrame:
    """
    Check for repetitions inside the file
    :param symbol: Stock market symbol
    :param file_name: File name to be concatenated
    :param new_data: New data to be added to the existing file
    :return: Pandas DataFrame with concatenated data
    """
    # Read data from the csv file
    file_data = pd.read_csv(Path(config.DATA_DICT, symbol, file_name), index_col=False)
    # Extract dates from the DataFrame
    file_latest_dates = file_data['Date'][:len(new_data)].values
    for date in file_latest_dates:
        for line in new_data['Date']:
            if line == date:
                # Drop repetitions from the old data
                file_data.drop(file_data[file_data['Date'] == date].index, inplace=True)
    return pd.concat([new_data, file_data])


def find_oldest_file(symbol_dict: Path, frequency: str) -> (datetime.date, str, datetime.now().date):
    """
    Take all the files in given frequency and return information about the longest one
    :param symbol_dict: Path to the symbol directory
    :param frequency: String specifying the frequency of the data, possible values: [1d, 1wk, 1mo]
    :return: Tuple with: the end date of file, name of the file with the longest range and the start date of the oldest file
    """
    # Variables for date of file with the oldest data and file name
    oldest_file = datetime.now().date()
    name_of_longest_range_file: str = ''

    # Create list of all files inside symbol directory
    all_files = [item for item in os.listdir(symbol_dict) if os.path.isfile(Path(symbol_dict, item))]
    for file in all_files:
        file_freq: str = file.split('=')[1].split('.')[0]
        file_start, file_end = extract_date_from_file(file)
        if file_freq == frequency:
            if file_start < oldest_file:
                if file.split('_')[1] == 'oldest':
                    oldest_file = 'oldest_' + str(file_start)
                    name_of_longest_range_file = file
                    break
                else:
                    oldest_file = file_start
                    name_of_longest_range_file = file
    if name_of_longest_range_file == '':
        print(f'No data in "{frequency}" frequency')
        return
    else:
        # Setup new range to download the data
        final_file_start, final_file_end = extract_date_from_file(name_of_longest_range_file)
    return final_file_end, name_of_longest_range_file, oldest_file


def update_historical_data(symbols: Union[str, List[str]], frequency: str) -> None:
    """
    Update files with latest stock market data
    :param symbols: Stock symbol, accepts a single symbol or a list of symbols
    :param frequency: String specifying the frequency of the data, possible values: [1d, 1wk, 1mo]
    """
    current_day = datetime.now().date()
    if isinstance(symbols, str):
        # Path to the symbol directory
        symbol_dict: Path = Path(config.DATA_DICT, symbols)
        try:
            final_file_end, name_of_longest_range_file, oldest_file = find_oldest_file(symbol_dict, frequency)
        # Except whether wrong frequency was given
        except TypeError:
            return
        if current_day == final_file_end:
            print('Nothing to update, file is up-to-date')
        else:
            # Create Pandas DataFrame with latest data
            new_data: pd.DataFrame = download_historical_data(symbols, final_file_end.strftime('%Y-%m-%d'),
                                                              current_day.strftime('%Y-%m-%d'),
                                                              frequency=frequency, save_csv=False, update=True)
            # Concatenate data and check for repetitions
            updated_data = file_latest_data_checker(symbol=symbols, file_name=name_of_longest_range_file,
                                                    new_data=new_data)
            # Save file with updated stock information
            file_name: str = f'{symbols}_{oldest_file}-{current_day}&freq={frequency}.csv'
            updated_data.to_csv(Path(config.DATA_DICT, symbols, file_name), index_label=False, index=False)

        # Review all files inside symbol directory
        delete_files_with_less_data_range(symbols)
    elif isinstance(symbols, list):
        latest_end_date: datetime.date = datetime.now().date()
        list_of_longest_range_file: list[str] = []
        symbols_to_update: list[str] = []
        list_of_oldest_file: dict[str: datetime.date] = {}
        for symbol in symbols:
            # Path to the symbol directory
            symbol_dict: Path = Path(config.DATA_DICT, symbol)
            try:
                final_file_end, name_of_longest_range_file, oldest_file = find_oldest_file(symbol_dict, frequency)
            # Except whether wrong frequency was given
            except TypeError:
                # Assign further variables to empty string
                final_file_end, name_of_longest_range_file, oldest_file = '', '', ''
                pass
            try:
                if final_file_end < current_day:
                    symbols_to_update.append(symbol)
                    list_of_longest_range_file.append(name_of_longest_range_file)
                    list_of_oldest_file[symbol] = oldest_file
                if final_file_end < latest_end_date:
                    latest_end_date = final_file_end
            except TypeError:
                pass

        if current_day == latest_end_date:
            print('Nothing to update, file is up-to-date')
        else:
            # Create Pandas DataFrame with latest data
            new_data: pd.DataFrame = download_historical_data(symbols_to_update, latest_end_date.strftime('%Y-%m-%d'),
                                                              current_day.strftime('%Y-%m-%d'),
                                                              frequency=frequency, save_csv=False, update=True)

            for symbol, symbol_oldest, symbol_file_name in zip(symbols_to_update, list_of_oldest_file.values(),
                                                               list_of_longest_range_file):
                symbol_data = new_data[new_data['Company'] == symbol].drop(columns='Company')
                # Concatenate data and check for repetitions
                updated_data = file_latest_data_checker(symbol=symbol, file_name=symbol_file_name, new_data=symbol_data)

                # Save file with updated stock information
                file_name: str = f'{symbol}_{symbol_oldest}-{current_day}&freq={frequency}.csv'
                updated_data.to_csv(Path(config.DATA_DICT, symbol, file_name), index_label=False, index=False)

                # Review all files inside symbol directory
                delete_files_with_less_data_range(symbol)


if __name__ == '__main__':
    pass
    # download_historical_data(symbol='NVDA', start='2000-07-08',
    #                          end=datetime.now().date().strftime('%Y-%m-%d'), frequency='1d')

    # download_historical_data(symbols='QWS', start='2020-07-08', end='2023-07-12', frequency='1d')
    # download_historical_data(symbols='NKLA', start='2023-07-20', end='2023-07-21')

    # Symbol list download tests
    # download_historical_data(symbols=['NKLA', 'AAPL'], start='2023-07-09', end='2023-07-22')
    # download_historical_data(symbols=['NKLA', 'AAPL', 'AMD', 'MSFT'], start='2023-06-08', end='2023-07-15')
    # download_historical_data(symbols=['MSFT'], start='2023-07-10', end='2023-07-22')

    # download_historical_data(symbols=['RIVN', 'SOFI', 'NKLA', 'AAPL', 'AMD', 'MSFT'], start='2023-06-08',
    #                          end='2023-07-15', save_csv=False)

    # download_historical_data(symbols=['RIVN', 'SOFI', 'NKLA', 'AAPL', 'AMD', 'MSFT'], start='2022-06-08',
    #                          end='2023-07-15')

    # Symbol list update tests
    # update_historical_data('AAPL', '1d')
    # update_historical_data(['NKLA', 'AAPL'], '1d')
    # update_historical_data(['NKLA', 'AAPL'], '1mo')
    # update_historical_data('NKLA', '1mo')
    # update_historical_data(['NKLA', 'AAPL'], '1d')  # Fix problem with unknown stock symbol ('GOOGL')

    # update_historical_data(['NVDA', 'TSLA'], '1d')

    # update_historical_data(symbols=['RIVN', 'SOFI', 'NKLA', 'AAPL', 'AMD', 'MSFT'], frequency='1d')

    # Tests for stock_symbols file
    df = pd.read_csv(Path(config.DATA_DICT, 'stock_symbols.csv'), header=None, index_col=0)
    stock_symbols = df[1].values
    download_historical_data(symbols=stock_symbols, start='1985-01-01', end='2023-07-01')
