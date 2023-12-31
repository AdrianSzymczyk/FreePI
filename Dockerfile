FROM python:3.11

WORKDIR /code

COPY ./requirements.txt /code/requirements.txt
COPY ./data/stock_symbols.csv /code/data/stock_symbols.csv
COPY ./config /code/config

RUN pip install --no-cache-dir --upgrade -r /code/requirements.txt

RUN apt-get install -y wget
RUN wget -q -O - https://dl-ssl.google.com/linux/linux_signing_key.pub | apt-key add - \
    && echo "deb http://dl.google.com/linux/chrome/deb/ stable main" >> /etc/apt/sources.list.d/google.list
RUN apt-get update && apt-get -y install google-chrome-stable

COPY ./backend /code/backend
COPY ./webScrape /code/webScrape
COPY ./data/stock_database.db /code/data/stock_database.db

CMD ["uvicorn", "backend.api:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]

# Need a database to retrive the data
# check the webdriver on docker