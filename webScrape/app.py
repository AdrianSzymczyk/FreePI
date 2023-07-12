import os
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import List
from config import config
import numpy as np
import pandas as pd
import re

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


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
    chr_driver.set_page_load_timeout(5)
    return chr_driver


def initial_run(driver: webdriver, cookie_btn_path: str = '//*[@id="consent-page"]/div/div/div/form/div[2]/div['
                                                          '2]/button[1]', consent: bool = False):
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


def date_and_freq_check(symbol: str, input_start_date: datetime, input_end_date: datetime, frequency: str) -> bool:
    """
    Check whether the given date range is covered by existing files.
    :param symbol: String representing the stock symbol
    :param input_start_date: Beginning of the period of time, valid format: "2021-09-08"
    :param input_end_date: End of the period of time, valid format: "2021-08-08"
    :param frequency: String defining the frequency of the data
    :return: Bool value whether to download new data
    """
    # Create variable with path to the symbol dictionary
    dict_path = Path(config.DATA_DICT, symbol)
    # Convert date
    input_start_date = input_start_date.date()
    input_end_date = input_end_date.date()
    # Create list with all files inside the dictionary
    all_files = [item for item in os.listdir(dict_path) if os.path.isfile(os.path.join(dict_path, item))]

    for file in all_files:
        file_start, file_end = extract_date_from_file(file)
        file_freq: str = file.split('=')[1].split('.')[0]
        if file_start <= input_start_date <= file_end:
            if file_start <= input_end_date <= file_end:
                if file_freq == frequency:
                    return True
    return False


def download_historical_data(symbol: str, start: str, end: str, frequency: str = '1d') -> None:
    """
    Fetch stock market data from the yahoo finance over a given period.
    :param symbol: Stock symbol
    :param start: Beginning of the period of time, valid format: "2021-09-08"
    :param end: End of the period of time, valid format: "2021-08-08"
    :param frequency: String defining the frequency of the data, defaults-1d, possible values: [1d, 1wk, 1mo]
    """
    # Convert start and end time into datetime format
    start = datetime.strptime(start, '%Y-%m-%d')
    start_to_file = start.date()
    end = datetime.strptime(end, '%Y-%m-%d')
    end_to_file = end.date()

    if not date_and_freq_check(symbol=symbol, input_start_date=start, input_end_date=end, frequency=frequency):
        # Convert time strings to timestamp format
        start_time: int = int(start.replace(tzinfo=timezone.utc).timestamp())
        end_time: int = int(end.replace(tzinfo=timezone.utc).timestamp())
        historical_url = f'https://finance.yahoo.com/quote/{symbol}/history?period1={start_time}&period2={end_time}' \
                         f'&interval={frequency}&filter=history&frequency={frequency}&includeAdjustedClose=true'

        # Browse webpage and accept cookies
        driver = setup_webdriver()
        driver.get(historical_url)
        initial_run(driver)

        all_data_loaded: bool = False
        while not all_data_loaded:
            # Variables to detect not loading website
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
                # time.sleep(0.2)
                last_row = driver.find_element(By.CSS_SELECTOR, '#Col1-1-HistoricalDataTable-Proxy > section > div.Pb\(10px\).Ovx\(a\).W\(100\%\) > table > tbody > tr:last-child > td.Py\(10px\).Ta\(start\)')
                last_date = datetime.strptime(last_row.text, "%b %d, %Y").date()
                # Adjust lower and upper limits of last displayed date
                if frequency == '1wk':
                    lower_start_limit = start.date()-timedelta(days=7)
                    upper_start_limit = start.date() + timedelta(days=7)
                elif frequency == '1mo':
                    lower_start_limit = start.date() - timedelta(days=31)
                    upper_start_limit = start.date() + timedelta(days=31)
                else:
                    lower_start_limit = start.date() - timedelta(days=4)
                    upper_start_limit = start.date() + timedelta(days=4)
                # Check whether all the data loaded
                if lower_start_limit < last_date < upper_start_limit:
                    all_data_loaded = True
                    break
                elif str(last_date) == str(tmp_last_date):
                    print('Endless loop', last_date, '=', tmp_last_date)
                    endless_loop = True
                    break
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
        df = pd.DataFrame(final_list[1:], columns=final_list[0])

        # Create sub folder for stock symbol whether it doesn't exist
        os.makedirs(Path(config.DATA_DICT, symbol), exist_ok=True)
        # Save downloaded data into csv file
        file_name: str = f'{symbol}_{start_to_file}-{end_to_file}&freq={frequency}.csv'
        df.to_csv(Path(config.DATA_DICT, symbol, file_name), index_label=False, index=False)

        # Quit the webdriver and close the browser
        driver.quit()
    else:
        print('Data in the given date range already exists')


def embrace_files(symbol: str) -> None:
    """
    Embrace all the files and decide if delete any files.
    :param symbol: Stock symbol
    :return:
    """
    try:
        # Create path and list of all files inside the symbol dictionary
        dict_path: Path = Path(config.DATA_DICT, symbol)
        all_files = [item for item in os.listdir(dict_path) if os.path.isfile(Path(dict_path, item))]
        # Loop over the list with files names
        for file in all_files:
            # Create an empty list of files to be deleted
            delete_list: List[str] = []
            # Create a variables with a date range
            file_start, file_end = extract_date_from_file(file)
            # Create a variable with frequency of the data
            frequency: str = file.split('=')[1].split('.')[0]
            for inside_file in all_files:
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
                        os.remove(Path(dict_path, inside_file))
                    elif file_start <= inside_file_start and file_end > inside_file_end and frequency == inside_freq:
                        print(f'2. Removed file: {inside_file}')
                        delete_list.append(inside_file)
                        os.remove(Path(dict_path, inside_file))
            for elem in delete_list:
                all_files.remove(elem)

    # Raise exception if the directory for the symbol does not exist
    except FileNotFoundError:
        print(f'Dictionary for "{symbol}" was not found')


def extract_date_from_file(file: str) -> (datetime.date, datetime.date):
    """
    Get the range of the data.
    :param file: Name of the file
    :return: Start and end dates from the file name
    """
    file_date = file.split('_')[1].split('&')[0]
    file_start = datetime.strptime(file_date[:10], '%Y-%m-%d').date()
    file_end = datetime.strptime(file_date[11:], '%Y-%m-%d').date()
    return file_start, file_end


if __name__ == '__main__':
    pass
    download_historical_data(symbol='NVDA', start='2010-07-08', end='2023-07-12', frequency='1mo')
    download_historical_data(symbol='NVDA', start='2015-01-01', end='2023-07-12', frequency='1d')
    embrace_files('NVDA')
