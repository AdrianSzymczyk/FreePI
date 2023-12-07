import os
import sqlite3
from typing import List
import pandas as pd
import pytest
from webScrape import db_controller
from pathlib import Path


@pytest.fixture
def data_directory():
    """Return data absolute path."""
    DEFAULT_DICT = Path(__file__).parent.parent.parent.absolute()
    return DEFAULT_DICT / 'data'


@pytest.fixture(scope='module')
def data():
    df = pd.DataFrame(
        {
            'Date': ['2010-07-01'],
            'Open': [1.67],
            'High': [1.73],
            'Low': [1.0],
            'Close': [1.33],
            'Adj Close': [1.33],
            'Volume': [968637000]
        }
    )
    return df


def create_db_connection(data_directory, database_name: str) -> sqlite3.Connection:
    """Create and return connection to the test database."""
    try:
        conn = sqlite3.connect(Path(data_directory, database_name))
        return conn
    except sqlite3.Error as e:
        print(e)
        return None


def delete_db(data_directory):
    """Delete the entire database."""
    try:
        os.remove(Path(data_directory, 'test_database.db'))
    except FileNotFoundError:
        print("Database does not exists!")


def get_database_data(connection: sqlite3.Connection, table_name: str) -> List:
    """Retrieve data from the database table."""
    retrieve_query: str = f' SELECT * FROM {table_name} ORDER BY Date DESC'
    cursor = connection.execute(retrieve_query)
    return cursor.fetchall()


@pytest.mark.database
def test_db_connect(data_directory):
    """Test connection with test database."""
    conn = create_db_connection(data_directory, 'test_database.db')
    assert isinstance(conn, sqlite3.Connection)
    conn.close()


@pytest.mark.database
def test_db_df_insert(data_directory, data):
    """Test insertion into test database."""
    database_name: str = 'test_database'
    conn = create_db_connection(data_directory, database_name + '.db')
    data.to_sql('test_table', conn, if_exists='append', index=False)
    result = pd.DataFrame([get_database_data(conn, 'test_table')[0]], columns=data.columns.values)
    assert data['Date'][0] == result['Date'][0]
    assert data['Open'][0] == result['Open'][0]
    assert data['High'][0] == result['High'][0]
    assert data['Low'][0] == result['Low'][0]
    assert data['Close'][0] == result['Close'][0]
    assert data['Adj Close'][0] == result['Adj Close'][0]
    assert data['Volume'][0] == result['Volume'][0]
    conn.close()


@pytest.mark.database
def test_db_duplicates(data_directory, data):
    """Test duplicates detection from the test table."""
    database_name: str = 'test_database'
    conn = create_db_connection(data_directory, database_name + '.db')
    for _ in range(2):
        data.to_sql('test_table', conn, if_exists='append', index=False)
    db_controller.delete_duplicates(conn, 'test_table')
    results = get_database_data(conn, 'test_table')
    assert len(results) == 1
    conn.close()
