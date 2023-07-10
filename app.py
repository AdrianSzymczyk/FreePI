import os
import time
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
    Create and configure webdriver options and add extensions
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
    return chr_driver


def initial_run(driver: webdriver, cookie_btn_path: str = '//*[@id="consent-page"]/div/div/div/form/div[2]/div['
                                                             '2]/button[1]', consent: bool = False):
    """
    Accept cookies when scraper first launches
    :param driver: Webdriver for remote control and browsing the webpage
    :param cookie_btn_path: String defining XPath to consent button, defaults: "//*[@id="consent-page"]/div/div/div/form/div[2]/div[2]/button[1]"
    :param consent: Determines whether cookies was accepted, defaults: False
    :return:
    """
    if not consent:
        try:
            button = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.XPATH, cookie_btn_path))
            )
            driver.find_element(By.XPATH, cookie_btn_path).click()
            consent = True
        except TimeoutError:
            print('No cookie accept button')
    return consent


def date_check(symbol: str, input_start_date: datetime, input_end_date: datetime) -> bool:
    """
    Check whether the given date range is covered by existing files
    :param symbol: String representing the stock symbol
    :param input_start_date: Beginning of the period of time, valid format: "2021-09-08"
    :param input_end_date: End of the period of time, valid format: "2021-08-08"
    :return: Bool value whether to download new data
    """
    # Create variable with path to the symbol dictionary
    dict_path = Path(config.DATA_DICT, symbol)
    # Convert date
    start = input_start_date.date()
    end = input_end_date.date()
    # Create list with all files inside the dictionary
    all_files = [item for item in os.listdir(dict_path) if os.path.isfile(os.path.join(dict_path, item))]

    for file in all_files:
        file_date = file.split('_')[1].split('.')[0]
        file_start = datetime.strptime(file_date[:10], '%Y-%m-%d').date()
        file_end = datetime.strptime(file_date[11:], '%Y-%m-%d').date()

        print(f'File {file}')
        if file_start <= start <= file_end:
            if file_start <= end <= file_end:
                print(f'{start} -> PASSED')
                return True
    return False


def get_historical_data(symbol: str, start: str, end: str, frequency: str = '1d') -> None:
    """
    Fetch stock market data from the yahoo finance over a given period
    :param symbol: Stock symbol
    :param start: Beginning of the period of time, valid format: "2021-09-08"
    :param end: End of the period of time, valid format: "2021-08-08"
    :param frequency: String defining the frequency of the data, defaults-1d, possible values: [1d, 1wk, 1mo]
    """

    # Convert start and end time into datetime format
    start = datetime.strptime(start, '%Y-%m-%d')
    end = datetime.strptime(end, '%Y-%m-%d')

    # Check if passed days are not Saturday or Sundays
    if start.weekday() == 5:
        start = start + timedelta(days=2)
    elif start.weekday() == 6:
        start = start + timedelta(days=1)
    elif end.weekday() == 5:
        end = end + timedelta(days=2)
    elif end.weekday() == 6:
        end = end + timedelta(days=1)

    if not date_check(symbol=symbol, input_start_date=start, input_end_date=end):
        # Convert time strings to timestamp format
        start_time: int = int(start.replace(tzinfo=timezone.utc).timestamp())
        end_time: int = int(end.replace(tzinfo=timezone.utc).timestamp())
        historical_url = f'https://finance.yahoo.com/quote/{symbol}/history?period1={start_time}&period2={end_time}' \
                         f'&interval={frequency}&filter=history&frequency={frequency}&includeAdjustedClose=true'

        # Browse webpage and accept cookies
        driver = setup_webdriver()
        driver.get(historical_url)
        initial_run(driver)

        tmp_last_date = end
        i = 0
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.XPATH, '//*[@id="Col1-1-HistoricalDataTable-Proxy"]/section/div[2]/table'))
        )
        while True:
            driver.execute_script('window.scrollTo(0, document.getElementById("render-target-default").scrollHeight);')
            time.sleep(0.2)
            last_row = driver.find_element(By.CSS_SELECTOR,
                                           '#Col1-1-HistoricalDataTable-Proxy > section > div.Pb\(10px\).Ovx\(a\).W\('
                                           '100\%\) > table > tbody > tr:last-child > td.Py\(10px\).Ta\(start\).Pend\('
                                           '10px\)')
            last_date = datetime.strptime(last_row.text, "%b %d, %Y").date()
            if str(last_date) == str(start.date()):
                print(last_date, '==', str(start.date()))
                break
            elif str(last_date) == str(tmp_last_date):
                print('Endless loop')
                break
            i += 1
            if i > 5:
                tmp_last_date = last_date
        stock_table = driver.find_element(By.XPATH, '//*[@id="Col1-1-HistoricalDataTable-Proxy"]/section/div[2]/table')

        # Merge downloaded data into arrays and format it
        tmp_arr: np.array = np.array(stock_table.text.split('\n'))
        separated_data = [re.split(r'\s+(?!Close\*\*)', line) for line in tmp_arr[:-1]
                          if 'Dividend' not in line if 'Split' not in line]
        stock_data: List = []
        for i in range(1, len(separated_data)):
            date = ' '.join(separated_data[i][:3])
            stock_data.append([date] + separated_data[i][3:])
        final_list = [separated_data[0]] + stock_data
        # Join created arrays into Pandas DataFrame
        df = pd.DataFrame(final_list[1:], columns=final_list[0])

        # Create sub folder for stock symbol whether it doesn't exist
        os.makedirs(Path(config.DATA_DICT, symbol), exist_ok=True)
        # Save downloaded data into csv file
        file_name: str = f'{symbol}_{start.date()}-{end.date()}.csv'
        df.to_csv(Path(config.DATA_DICT, symbol, file_name), index_label=False, index=False)

        # Quit the webdriver and close the browser
        driver.quit()
    else:
        print('Data in the given date range already exists')


if __name__ == '__main__':
    get_historical_data(symbol='NVDA', start='2021-07-06', end='2023-07-07', frequency='1d')
