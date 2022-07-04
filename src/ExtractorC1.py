import configparser
import logging
from pathlib import Path

import pandas

import util

dir_to_search = Path('../data/C1_2908/')


def main():

    config = configparser.ConfigParser()
    config.read(Path('../config.ini').resolve())
    log = logging.getLogger('chl')

    log.debug(f'searching {dir_to_search.resolve()} for C1 data...')
    df_combined = pandas.DataFrame()
    for file in dir_to_search.iterdir():
        df = pandas.read_csv(file.resolve())
        log.debug(f'\tfile: "{file.name}" contains {len(df):,} rows')
        df_combined = pandas.concat([df_combined, df])

    column_rename = {
        'Transaction Date': 't_date',
        'Posted Date': 'posted_date',
        'Card No.': 'card_no',
        'Description': 't_description',
        'Category': 'category',
        'Debit': 'debit',
        'Credit': 'credit',
    }
    df_combined.rename(columns=column_rename, inplace=True)
    df_combined['debit'] = df_combined['debit'].fillna(0)
    df_combined['credit'] = df_combined['credit'].fillna(0)
    df_combined = df_combined.fillna('')

    log.debug(f'final data count it {len(df_combined):,} rows')
    log.debug(f'\n{df_combined.to_markdown(index=False)}')

    db_conn = util.create_database_connection(
        host=config['chl_mysql']['host'],
        port=config['chl_mysql']['port'],
        username=config['chl_mysql']['username'],
        password=config['chl_mysql']['password'],
        database='sys',
    )

    df_combined.to_sql(
        con=db_conn,
        schema='FinData',
        name='TransactionsC1',
        if_exists='append',
        index=False,
    )


if __name__ == '__main__':
    util.create_log()
    main()
