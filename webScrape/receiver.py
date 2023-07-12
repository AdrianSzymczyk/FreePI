import os
from datetime import datetime
import pandas as pd
from webScrape import app
from pathlib import Path
from config import config
from typing import List


def receive_data(symbol: str, start: str, end: str, frequency: str = '1d') -> pd.DataFrame:
    """
    Return data from a date range from a specific stock symbol
    :param symbol:
    :param symbol: Stock symbol
    :param start: Beginning of the period of time, valid format: "2021-09-08"
    :param end: End of the period of time, valid format: "2021-08-08"
    :param frequency: String defining the frequency of the data, defaults-1d, possible values: [1d, 1wk, 1mo]
    :return: Pandas DataFrame with stock data from a date range
    """
    # Convert passed start and end dates into datetime.date format
    start_date = datetime.strptime(start, '%Y-%m-%d').date()
    end_date = datetime.strptime(end, '%Y-%m-%d').date()

    # Setup path to the directory
    dict_path: Path = Path(config.DATA_DICT, symbol)
    while True:
        # Create a list of all files inside the symbol directory
        all_fies: List[str] = [item for item in os.listdir(dict_path) if os.path.isfile(Path(dict_path, item))]
        for file in all_fies:
            file_start, file_end = app.extract_date_from_file(file)
            if start_date >= file_start:
                if end_date <= file_end:
                    data = pd.read_csv(Path(dict_path, file), index_col=False)
                    # Convert date column into datetime.date format
                    data['Date'] = data['Date'].apply(lambda x: datetime.strptime(x, '%b %d, %Y').date())
                    # Get data from the file in a date range
                    final_data = data[(data['Date'] >= start_date) & (data['Date'] <= end_date)]
                    return final_data
                else:
                    app.download_historical_data(symbol, start, end, frequency)
            else:
                app.download_historical_data(symbol, start, end, frequency)


if __name__ == "__main__":
    print(receive_data('NVDA', '2020-01-01', '2023-07-08'))
    print(receive_data('NVDA', '2015-01-01', '2023-07-12'))