import sqlite3
import os
import shutil
import zipfile
from pathlib import Path
from webScrape import app
from config import config
import pandas as pd
from typing import Union, List
from datetime import datetime



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
    # Execute duplicates query
    cursor.execute(delete_duplicate_query)
    # Commit changes in database
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
    database_table_name: str = app.get_name_of_symbol_table(symbol, frequency, connection, database_name)
    if database_table_name is not None:
        table_start, table_end = app.extract_date_from_table(database_table_name)
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


def fetch_from_database(symbol: str, frequency: str, connection: sqlite3.Connection | None = None,
                        database_name: str = 'stock_database.db') -> None:
    """
    Fetch and display data from the database symbol table
    :param connection: Connection to the database.
    :param symbol: Stock symbol, accepts a single symbol or a list of symbols.
    :param frequency: String specifying the frequency of the data, defaults-1d, possible values: [1d, 1wk, 1mo].
    :param database_name: Name of the database where data will be saved. Default "stock_database".
    """
    if connection is None:
        connection = sqlite3.connect(f'{Path(config.DATA_DICT, "stock_database.db")}')
    try:
        # Get the name of the symbol array
        table_name: str = app.get_name_of_symbol_table(symbol, frequency, connection, database_name)
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


def reset_database() -> None:
    """Reset database by deleting it and creating new one"""
    try:
        # Create a backup of the database
        backup_database()
        # Remove current working database
        os.remove(Path(config.DATA_DICT, 'stock_database.db'))
    except FileNotFoundError:
        print('Database does not exist!')
    # Create a new empty database
    conn = sqlite3.connect(f'{Path(config.DATA_DICT, "stock_database.db")}')
    conn.close()


def backup_database() -> None:
    """Create a backup version of the database"""
    current_day = datetime.now()
    database_path: Path = Path(config.DATA_DICT, 'stock_database.db')
    backup_path: Path = Path(config.DATA_DICT, 'backups', f'backup_database_{current_day.date()}.db')
    # Copy current database data to the backup database
    if os.path.isfile(database_path):
        shutil.copy2(database_path, backup_path)


def manage_backups() -> None:
    # Get current year and month
    current_month_year: datetime.date = datetime.now().strftime('%Y-%m')
    current_year: str = current_month_year[:4]
    current_month: str = current_month_year[5:]

    # Create paths for new and existing data
    backup_path: Path = Path(config.DATA_DICT, 'backups')
    zipfile_path: Path = Path(backup_path, current_year, current_month)
    if not os.path.isdir(zipfile_path):
        os.makedirs(zipfile_path)

    # Get all files inside backup directory
    databases_array: List[str] = os.listdir(backup_path)
    zipfile_name: str = f'backup_{current_month_year}.zip'
    # Create a zip file with the specified name
    with zipfile.ZipFile(Path(zipfile_path, zipfile_name), 'w') as zip_file:
        # Iterate through files in the directory
        for db_file in databases_array:
            try:
                database_date = db_file.split('_')[2][:10]
                if current_month_year == database_date[0:7]:
                    file_path: Path = Path(config.DATA_DICT, 'backups', db_file)
                    # Add the file to the zip file
                    zip_file.write(file_path, os.path.relpath(file_path, backup_path))
            except IndexError:
                pass


def display_database_tables() -> None:
    """Display names of all the database tables"""
    # Connect to the database
    conn = sqlite3.connect(f'{Path(config.DATA_DICT, "stock_database.db")}')
    try:
        cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table';")
        # Fetch everything from the database
        tables = cursor.fetchall()
        if tables:
            # Print all table names
            print('\nTables in the database')
            for table in tables:
                print(table[0])
        else:
            print('No tables in the database')
    except sqlite3.Error as e:
        print(f'Error: {e}')
    finally:
        conn.close()


if __name__ == "__main__":
    manage_backups()
