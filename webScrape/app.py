import functools
import os
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import List
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
    options = webdriver.ChromeOptions()
    options.add_extension(Path(config.EXTENSIONS_DICT, 'u_block_extension.crx'))
    options.add_experimental_option('detach', True)
    options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                         'Chrome/114.0.0.0 Safari/537.36')
    # Adding argument to disable the AutomationControlled flag
    options.add_argument("--disable-blink-features=AutomationControlled")

    # Exclude the collection of enable-automation switches
    options.add_experimental_option("excludeSwitches", ["enable-automation"])

    # Turn-off userAutomationExtension
    options.add_experimental_option("useAutomationExtension", False)
    chr_driver = webdriver.Chrome(options=options)
    chr_driver.set_page_load_timeout(7)
    return chr_driver


def initial_driver_run(driver: webdriver, cookie_btn_path: str = '//*[@id="consent-page"]/div/div/div/form/div[2]/div[2]/button[1]', consent: bool = False):
    """
    Accept cookies when scraper first launches.
    :param driver: Webdriver for remote control and browsing the webpage
    :param cookie_btn_path: String defining XPath to consent button, defaults: "//*[@id="consent-page"]/div/div/div/form/div[2]/div[2]/button[1]"
    :param consent: Determines whether cookies was accepted, defaults: False
    :return:
    """
    if not consent:
        try:
            WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.XPATH, cookie_btn_path))
            )
            driver.find_element(By.XPATH, cookie_btn_path).click()
            consent = True
        except TimeoutError:
            print('No cookie accept button')
    return consent


def extract_date_from_file(file: str) -> (datetime.date, datetime.date):
    """
    Get the range of the data.
    :param file: Name of the file
    :return: Start and end dates from the file name
    """
    # Check whether the data comes from the longest range
    if len(file.split('_')) > 2:
        file_date = file.split('_')[2].split('&')[0]
    else:
        file_date = file.split('_')[1].split('&')[0]
    file_start = datetime.strptime(file_date[:10], '%Y-%m-%d').date()
    file_end = datetime.strptime(file_date[11:], '%Y-%m-%d').date()
    return file_start, file_end


def create_files_list(func):
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


@create_files_list
def date_and_freq_check(input_start_date: datetime, input_end_date: datetime, frequency: str, **kwargs) -> bool:
    """
    Check whether the given date range is covered by existing files.
    :param input_start_date: Beginning of the period of time, valid format: "2021-09-08"
    :param input_end_date: End of the period of time, valid format: "2021-08-08"
    :param frequency: String defining the frequency of the data
    :return: Bool value whether to download new data
    """
    # Convert date
    input_start_date = input_start_date.date()
    input_end_date = input_end_date.date()

    for file in kwargs['all_files']:
        file_start, file_end = extract_date_from_file(file)
        file_freq: str = file.split('=')[1].split('.')[0]
        if file_start <= input_start_date <= file_end:
            if file_start <= input_end_date <= file_end:
                if file_freq == frequency:
                    pass
                    return True
    return False


def download_historical_data(symbol: str, start: str, end: str, frequency: str = '1d', save_csv: bool = True) \
        -> pd.DataFrame:
    """
    Fetch stock market data from the yahoo finance over a given period.
    :param symbol: Stock symbol
    :param start: Beginning of the period of time, valid format: "2021-09-08"
    :param end: End of the period of time, valid format: "2021-08-08"
    :param frequency: String specifying the frequency of the data, defaults-1d, possible values: [1d, 1wk, 1mo]
    :param save_csv: Determine whether to save csv file. Default True
    """
    # Upper case symbol
    symbol = symbol.upper()
    try:
        # Convert start and end time into datetime format
        start = datetime.strptime(start, '%Y-%m-%d')
        start_to_file = start.date()
        end = datetime.strptime(end, '%Y-%m-%d')
        end_to_file = end.date()
        # Validate passed arguments
        if start_to_file > end_to_file:
            logger.info('Start date is greater than end date')
            return
        # Change end date to actual date if given date is out of range
        if end_to_file > datetime.now().date():
            end_to_file = datetime.now().date()
    except ValueError as err:
        logger.error(err)
        return

    if not date_and_freq_check(symbol=symbol, input_start_date=start, input_end_date=end, frequency=frequency):
        # Convert time strings to timestamp format
        start_time: int = int(start.replace(tzinfo=timezone.utc).timestamp())
        end_time: int = int(end.replace(tzinfo=timezone.utc).timestamp())
        historical_url = f'https://finance.yahoo.com/quote/{symbol}/history?period1={start_time}&period2={end_time}' \
                         f'&interval={frequency}&filter=history&frequency={frequency}&includeAdjustedClose=true'

        # Browse webpage and accept cookies
        driver = setup_webdriver()
        try:
            driver.get(historical_url)
            initial_driver_run(driver)
        except selenium.common.exceptions.TimeoutException:
            print('Timed out receiving message')
            driver.refresh()

        all_data_loaded: bool = False
        while not all_data_loaded:
            # Variables to detect not loading website
            loading_message: str = '#Col1-1-HistoricalDataTable-Proxy > section > div.Pb\(10px\).Ovx\(a\).W\(100\%\) > div'
            tmp_last_date: datetime.date = end.date()
            endless_loop: bool = False
            i: int = 0
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located(
                    (By.XPATH, '//*[@id="Col1-1-HistoricalDataTable-Proxy"]/section/div[2]/table'))
            )
            while True:
                driver.execute_script(
                    'window.scrollTo(0, document.getElementById("render-target-default").scrollHeight);')
                time.sleep(0.2)
                last_row_date = driver.find_element(By.CSS_SELECTOR,
                                               '#Col1-1-HistoricalDataTable-Proxy > section > div.Pb\(10px\).Ovx\(a\).W\(100\%\) > table > tbody > tr:last-child > td.Py\(10px\).Ta\(start\)')
                last_date = datetime.strptime(last_row_date.text, "%b %d, %Y").date()
                # Adjust lower and upper limits of last displayed date
                if frequency == '1wk':
                    lower_start_limit = start.date() - timedelta(days=7)
                    upper_start_limit = start.date() + timedelta(days=7)
                elif frequency == '1mo':
                    lower_start_limit = start.date() - timedelta(days=31)
                    upper_start_limit = start.date() + timedelta(days=31)
                else:
                    lower_start_limit = start.date() - timedelta(days=4)
                    upper_start_limit = start.date() + timedelta(days=4)
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
                        start_to_file = last_date
                        break
                # Handle variables responsible for refreshing page when driver gets stuck
                i += 1
                if i > 10:
                    tmp_last_date = last_date

            if endless_loop:
                print('Refreshing page!!!')
                driver.refresh()
        stock_table = driver.find_element(By.XPATH, '//*[@id="Col1-1-HistoricalDataTable-Proxy"]/section/div[2]/table')

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
        # Create sub folder for stock symbol whether it doesn't exist
        os.makedirs(Path(config.DATA_DICT, symbol), exist_ok=True)

        # Quit the webdriver and close the browser
        driver.quit()

        # Save downloaded data into csv file
        if save_csv:
            file_name: str = f'{symbol}_{start_to_file}-{end_to_file}&freq={frequency}.csv'
            stock_df.to_csv(Path(config.DATA_DICT, symbol, file_name), index_label=False, index=False)
        else:
            return stock_df
    else:
        print('Data in the given date range already exists')

    # Review all files inside symbol directory
    delete_files_with_less_data_range(symbol)


@create_files_list
def delete_files_with_less_data_range(**kwargs) -> None:
    """Review all the files and decide whether to delete any of them."""
    try:
        # Loop over the list with files names
        for file in kwargs['all_files']:
            # Create an empty list of files to be deleted
            delete_list: List[str] = []
            # Create a variables with a date range
            file_start, file_end = extract_date_from_file(file)
            # Create a variable with frequency of the data
            frequency: str = file.split('=')[1].split('.')[0]
            for inside_file in kwargs['all_files']:
                if file == inside_file:
                    pass
                else:
                    # Variables for inside file
                    inside_file_start, inside_file_end = extract_date_from_file(inside_file)
                    inside_freq: str = inside_file.split('=')[1].split('.')[0]
                    # Remove file from the directory if data already exists
                    if file_start < inside_file_start and file_end >= inside_file_end and frequency == inside_freq:
                        print(f'1. Removed file: {inside_file}')
                        delete_list.append(inside_file)
                        os.remove(Path(kwargs['dict_path'], inside_file))
                    elif file_start <= inside_file_start and file_end > inside_file_end and frequency == inside_freq:
                        print(f'2. Removed file: {inside_file}')
                        delete_list.append(inside_file)
                        os.remove(Path(kwargs['dict_path'], inside_file))
            for elem in delete_list:
                kwargs['all_files'].remove(elem)

    # Raise exception if the directory for the symbol does not exist
    except FileNotFoundError:
        print(f'Directory for "{kwargs["symbol"]}" was not found')


@create_files_list
def update_historical_data(frequency: str, **kwargs) -> None:
    # TODO:
    #   - adapt function for all frequency at the same time - update all files one by one
    """
    Update files with latest stock market data
    :param frequency: String specifying the frequency of the data, possible values: [1d, 1wk, 1mo]
    """
    current_day: datetime.date = datetime.now().date()
    oldest_file: datetime.date = current_day
    name_of_longest_range_file: str = ''
    for file in kwargs['all_files']:
        file_freq: str = file.split('=')[1].split('.')[0]
        file_start, file_end = extract_date_from_file(file)
        if file_freq == frequency:
            if file_start < oldest_file:
                oldest_file = file_start
                name_of_longest_range_file = file
    # Setup new range to download the data
    final_file_start, final_file_end = extract_date_from_file(name_of_longest_range_file)

    if current_day == final_file_end:
        print('Nothing to update, file is up to date')
    else:
        new_data: pd.DataFrame = download_historical_data(kwargs['symbol'], final_file_end.strftime('%Y-%m-%d'),
                                                          current_day.strftime('%Y-%m-%d'), save_csv=False)
        # Read older data
        longest_file = pd.read_csv(Path(config.DATA_DICT, kwargs['symbol'], name_of_longest_range_file),
                                   index_col=False)
        # Concatenate new and old data
        updated_data = pd.concat([new_data, longest_file])

        # Save file with updated stock information
        file_name: str = f'{kwargs["symbol"]}_{oldest_file}-{current_day}&freq={frequency}.csv'
        updated_data.to_csv(Path(config.DATA_DICT, kwargs['symbol'], file_name), index_label=False, index=False)

    # Review all files inside symbol directory
    delete_files_with_less_data_range(kwargs['symbol'])


if __name__ == '__main__':
    pass
    # download_historical_data(symbol='NVDA', start='2000-07-08',
    #                          end=datetime.now().date().strftime('%Y-%m-%d'), frequency='1d')
    # delete_files_with_less_data_range('NVDA')

    # Tests for out of range dates
    # download_historical_data(symbol='TSLA', start='2005-12-31', end='2025-07-13')
    download_historical_data(symbol='NVDa', start='1997-12-31', end='2025-07-13')
    # update_historical_data('NVDA', '1d')
