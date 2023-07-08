import string
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from config import config

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains

import config.config


def start_and_setup_webdriver() -> webdriver:
    options = webdriver.ChromeOptions()
    options.add_extension(Path(config.EXTENSIONS_DICT, 'u_block_extension.crx'))
    chr_driver = webdriver.Chrome(options=options)
    return chr_driver


def initial_run(driver: webdriver, cookie_btn_path: string = '//*[@id="consent-page"]/div/div/div/form/div[2]/div['
                                                             '2]/button[1]', consent: bool = False):
    """

    :param driver:
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


# Adds the cookie into current browser context
# consent_btn = '//*[@id="consent-page"]/div/div/div/form/div[2]/div[2]/button[1]'
# consent = False

# driver.get('https://finance.yahoo.com/most-active')
# if not consent:
#     try:
#         button = WebDriverWait(driver, 10).until(
#             EC.element_to_be_clickable((By.XPATH, consent_btn))
#         )
#         driver.find_element(By.XPATH, consent_btn).click()
#         consent = True
#     except ValueError:
#         driver.quit()
#
# try:
#     element = WebDriverWait(driver, 10).until(
#         EC.presence_of_element_located((By.XPATH, '//*[@id="scr-res-table"]/div[1]/table'))
#     )
# finally:
#     driver.quit()


def get_historical_data(driver: webdriver, symbol: string, start: datetime, end: datetime,
                        frequency: string = '1d') -> None:
    """
    Fetch stock market data from the yahoo finance over a given period
    :param driver: Webdriver for browsing the webpage
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

    # Convert time strings to timestamp format
    start_time: int = int(start.replace(tzinfo=timezone.utc).timestamp())
    end_time: int = int(end.replace(tzinfo=timezone.utc).timestamp())
    historical_url = f'https://finance.yahoo.com/quote/{symbol}/history?period1={start_time}&period2={end_time}' \
                     f'&interval={frequency}&filter=history&frequency={frequency}&includeAdjustedClose=true'

    # Browse webpage and accept cookies
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
                                       '#Col1-1-HistoricalDataTable-Proxy > section > div.Pb\(10px\).Ovx\(a\).W\(100\%\) > table > tbody > tr:last-child > td.Py\(10px\).Ta\(start\).Pend\(10px\)')
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
    print(stock_table.text)
    driver.quit()


chr_driver = start_and_setup_webdriver()
get_historical_data(driver=chr_driver, symbol='NVDA', start='2019-07-06', end='2023-07-06', frequency='1d')
