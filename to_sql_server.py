import dask.dataframe as dd
import pandas as pd
import glob
import pyodbc as odbc
import time

from sqlalchemy.engine import Engine
# from sqlalchemy.engine import URL
from tqdm import tqdm
import sqlalchemy as sa

DRIVER = 'SQL Server Native Client 11.0'
# DRIVER = 'ODBC Driver 17 for SQL Server'
HOST = 'localhost'
SERVER = 'NB-R910JHBH\SA'
PORT = 1433
DATABASE = 'CitationNetwork'
UID = 'sa'
PWD = 'Pp12345678'

FILE = 'init-db.sql'

FILE_TO_NAME = {'[dbo].[Articles]': 'db_main_table',
                '[dbo].[Fos]': 'db_fos',
                '[dbo].[References]': 'db_references',
                '[dbo].[Keywords]': 'db_keywords',
                '[dbo].[Abstract]': 'db_abstract',
                '[dbo].[Authors]': 'db_authors',
                '[dbo].[ArticlesAuthors]': 'db_map_article_authors',
                '[dbo].[Orgs]': 'db_orgs',
                '[dbo].[Venues]': 'db_venue',
                '[dbo].[ArticlesVenues]': 'db_map_article_venue'}

COLUMNS_NAME = {'[dbo].[Fos]': ['article_id', 'fos'],
                '[dbo].[Articles]': ['id_article', 'title', 'year', 'n_citation_global', 'n_citation_local'],
                '[dbo].[Keywords]': ['keyword', 'article_id'],
                '[dbo].[Abstract]': ['abstract', 'article_id'],
                '[dbo].[Authors]': ['id_author', 'name'],
                '[dbo].[ArticlesAuthors]': ['id_article', 'id_author'],
                '[dbo].[Orgs]': ['org', 'id_author'],
                '[dbo].[Venues]': ['id_venue', 'name'],
                '[dbo].[ArticlesVenues]': ['id_article', 'id_venue'],
                '[dbo].[References]': ['reference', 'article_id']}


def connect_to_base(host=HOST,
                    port=PORT,
                    database=DATABASE,
                    driver=DRIVER,
                    uid=UID,
                    pwd=PWD) -> Engine:
    '''connect_url = URL.create(
        'mssql+pyodbc',
        username=uid,
        password=pwd,
        host=host,
        port=port,
        database=database,
        query=dict(driver=driver))'''
    my_url = f"mssql+pyodbc://{SERVER}/{DATABASE}?driver={DRIVER}?Trusted_Connection=yes"
    engine = sa.create_engine(my_url)
    print("Connection done!")
    return engine


def file_to_sql(engine: Engine, file: str = FILE) -> None:
    fd = open(file, 'r')
    sql_file = fd.read()
    fd.close()

    sql_commands = sql_file.split('------')

    with engine.connect() as con:
        for command in sql_commands:
            try:
                con.execute(command)
            except NotImplementedError as err:
                print(f"Command skipped: {err.__str__()}")


def truncate_db(engine: Engine) -> None:
    for table in list(FILE_TO_NAME.keys())[::-1]:
        engine.execute(f'DELETE FROM {table}')


def fill_table(engine: Engine, table_name: str, file_name: str) -> None:
    start = time.time()
    chunk_size = 100000
    i = 0
    j = 1
    engine.execute(f"DELETE FROM {table_name}")
    for df in pd.read_csv('files_test/' + file_name + '.csv', chunksize=chunk_size, iterator=True, sep=",",
                          index_col=0):
        df = df[COLUMNS_NAME[table_name]]
        df.index += j
        i += 1
        df.to_sql(name=table_name.split('.')[1].replace('[', '').replace(']', ''),
                  schema='dbo', con=engine, if_exists='append', index=False)
        j = df.index[-1] + 1
        print(f"{i}: {j} is done!")
    end = time.time()
    print(f'Data type: {file_name.split("_")[1]}, time: {end - start}')


def main():
    engine = connect_to_base()
    # if we need to create tables
    # file_to_sql(engine)
    # truncate tables
    truncate_db(engine)
    # fill tables
    for db, csv in FILE_TO_NAME.items():
        fill_table(engine, db, csv)
    print("DONE!")


if __name__ == '__main__':
    main()
