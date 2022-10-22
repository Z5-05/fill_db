import dask.dataframe as dd
import pandas as pd
import glob
import pyodbc as odbc
import time

from sqlalchemy.engine import Engine
from sqlalchemy.engine import URL
from tqdm import tqdm
import sqlalchemy as sa

# DRIVER = 'SQL Server Native Client 11.0'
DRIVER = 'ODBC Driver 17 for SQL Server'
HOST = 'localhost'
PORT = 1433
DATABASE = 'CitationNetwork'
UID = 'sa'
PWD = 'Pp12345678'

FILE = 'init-db.sql'

FILE_TO_NAME = {'[dbo].[Fos]': 'db_fos',
                '[dbo].[Articles]': 'db_main_table',
                '[dbo].[Keywords]': 'db_keywords',
                '[dbo].[Abstract]': 'db_abstract',
                '[dbo].[Authors]': 'db_authors',
                '[dbo].[ArticlesAuthors]': 'db_map_article_authors',
                '[dbo].[Orgs]': 'db_orgs',
                '[dbo].[Venues]': 'db_venue',
                '[dbo].[ArticlesVenues]': 'db_map_article_venue',
                '[dbo].[References]': 'db_references'}

COLUMNS_NAME = {'[dbo].[Fos]': ['id', 'fos', 'article_id'],
                '[dbo].[Articles]': ['id_article', 'title', 'year', 'n_citation_global', 'n_citation_local'],
                '[dbo].[Keywords]': ['id', 'keyword', 'article_id'],
                '[dbo].[Abstract]': ['id', 'abstract', 'article_id'],
                '[dbo].[Authors]': ['id_author', 'name'],
                '[dbo].[ArticlesAuthors]': ['id_article', 'id_author'],
                '[dbo].[Orgs]': ['id', 'org', 'id_author'],
                '[dbo].[Venues]': ['id_venue', 'name'],
                '[dbo].[ArticlesVenues]': ['id_article', 'id_venue'],
                '[dbo].[References]': ['id', 'reference', 'article_id']}


def connect_to_base(host=HOST,
                    port=PORT,
                    database=DATABASE,
                    driver=DRIVER,
                    uid=UID,
                    pwd=PWD) -> Engine:

    connect_url = URL.create(
        'mssql+pyodbc',
        username=uid,
        password=pwd,
        host=host,
        port=port,
        database=database,
        query=dict(driver=driver))
    engine = sa.create_engine(connect_url)
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


def fill_table(engine: Engine, table_name: str, file_name: str) -> None:

    start = time.time()
    chunk_size = 100
    i = 0
    j = 1
    engine.execute(f"DELETE FROM {table_name}")
    for df in pd.read_csv('files/' + file_name + '.csv', chunksize=chunk_size, iterator=True, sep=",", index_col=0):
        print(df.columns, COLUMNS_NAME[table_name])
        df = df[COLUMNS_NAME[table_name]]
        df.index += j
        i += 1
        df.to_sql(name=table_name, con=engine, if_exists='append', index=False)
        if i == 4:
            break
        j = df.index[-1] + 1
        print(f"{i}: {j} is done!")
    end = time.time()
    print(f'Data type: {file_name.split("_")[1]}, time: {end - start}')


def main():
    engine = connect_to_base()
    # if we need to create tables
    # file_to_sql(engine)
    # truncate tables
    # fill tables
    for db, csv in FILE_TO_NAME.items():
        fill_table(engine, db, csv)
    print("DONE!")


if __name__ == '__main__':
    main()
