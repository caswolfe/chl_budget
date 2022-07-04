import configparser
import logging
import re
from pathlib import Path
from tkinter import Tk, filedialog
from typing import Dict, List, Tuple

import pandas
from pdfminer.high_level import extract_text
from pdfminer.layout import LAParams

import util


def main():

    config = configparser.ConfigParser()
    config.read(Path('../config.ini'))
    log = logging.getLogger('chl')

    alias_map = util.create_aliases_map(config)

    Tk().withdraw()
    files = filedialog.askopenfilenames(
        initialdir=Path('../data/')
    )
    file_paths = [Path(f) for f in files]
    log.debug(f'selected files:')
    for fp in file_paths:
        log.debug(f'\t{fp.resolve()}')

    tbl_data = extract_batched(file_paths, alias_map)

    for acct, df in tbl_data.items():
        log.debug(f'writing account "{acct}" to csv...')
        df.to_csv(Path(f'../debug/{acct}.csv'), index=False)
        log.debug(f'\tcommitting to the database...')
        load_into_database(config, acct, df)
        log.debug(f'\tdone')


def extract_batched(pdf_files: List[Path], alias_map: Dict[str, str]) -> Dict[str, pandas.DataFrame]:
    """Extract tables from multiple files and aggregate them based off of the account ID"""

    log = logging.getLogger('chl')

    table_data = {}
    for fp in pdf_files:
        log.debug(f'batch extract from file: {fp.name}')
        partial_table_data = extract_from_pdf(fp, alias_map)
        for acct in partial_table_data.keys():
            ndf = partial_table_data[acct].copy()
            if acct not in table_data.keys():
                table_data[acct] = ndf
            else:
                table_data[acct] = pandas.concat([table_data[acct], ndf])

    log.debug(f'aggregated table data')
    for acct, df in table_data.items():
        log.debug(f'account: {acct}\n{df.to_markdown(index=False)}')

    return table_data


def extract_from_pdf(pdf_file: Path, alias_map: Dict[str, str]) -> Dict[str, pandas.DataFrame]:

    log = logging.getLogger('chl')
    log.info(f'extracting from file: {pdf_file.name} from {pdf_file.resolve()}')

    laparams = LAParams(
        char_margin=999999,
        line_margin=-.1
    )

    txt = extract_text(
        pdf_file=pdf_file.resolve(),
        laparams=laparams,
        # page_numbers=[0, 1, 2, 3, 4],
    )

    txt = txt.replace('\n', ' ')
    txt = re.sub(r'\s+', ' ', txt)
    txt = util.redact_aliases(alias_map, txt)

    # log.debug(f'txt:\n{txt}\n\n')

    return extract_from_text(txt)


def extract_from_text(text: str) -> Dict[str, pandas.DataFrame]:

    log = logging.getLogger('chl')
    rgx_group_date = r"(Member Number Statement Date .*? \d\d-\d\d-\d\d\d\d)"
    rgx_group_tables = r"(Account .*? Trans .*? The total number of days in this cycle)"
    rgx_extract = rgx_group_date + r"|" + rgx_group_tables

    current_year = ""
    table_data = {}
    groups_extract = re.findall(rgx_extract, text)
    for group in groups_extract:
        if group[0] != '':
            log.debug(f'{group[0]}')
            current_year = (re.findall(r"(\d\d-\d\d-\d\d\d\d)", group[0])[0])[-4:]
            log.debug(f'new current_year: {current_year}')
        elif group[1] != '':
            log.debug(f'{group[1]}')
            if current_year == "":
                raise Exception("Table found before date!")
            partial_table_data = extract_table_from_text(group[1], current_year)
            for acct, df in partial_table_data:
                ndf = df.copy()
                if acct not in table_data.keys():
                    table_data[acct] = ndf
                else:
                    table_data[acct] = pandas.concat([table_data[acct], df])
    return table_data


def extract_table_from_text(text: str, year: str) -> List[Tuple[str, pandas.DataFrame]]:

    log = logging.getLogger('chl')

    rgx_chck = r"(?:(?:ECU_Savings|ECU_Checking) Trans Date Eff Date Description Deposit Withdrawals Balance)" \
               r".*?(?:The total number of days in this cycle)"
    rgx_clmn = r"Trans Date Eff Date Description Deposit Withdrawals Balance"
    rgx_endg = r"The total number of days in this cycle"
    rgx_cntn = r"\(Continued on next page\).*?\(Continued\)"
    rgx_date = r"(\d\d-\d\d)"
    rgx_desc = r"(.*?)"
    rgx_curr = r"(\s?[+-]?[0-9]{1,3}(?:,?[0-9]{3})*\.[0-9]{2})"
    rgx_curr2 = rgx_curr + r"?"
    rgx_trns = rgx_date + rgx_desc + rgx_curr + rgx_curr2

    # extract tables from pdf text dump
    tbl_data = []
    log.debug(f'regex: "{rgx_trns}"')
    log.debug(f'text: "{text}"')
    tbl_results = re.findall(rgx_chck, text)
    log.debug(f'found {len(tbl_results)} tables to try')
    for i in range(len(tbl_results)):
        table = tbl_results[i]
        log.debug(f'trying:\n\t{table}')

        table = re.sub(rgx_clmn, '', table)
        table = re.sub(rgx_endg, '', table)
        table = re.sub(rgx_cntn, '', table)

        table_space_index = table.find(' ')
        log.debug(f'table_space_index: {table_space_index}')

        table_acct = table[:table_space_index]
        table = table[table_space_index:]

        tdata = {
            'Date': [],
            'Desc': [],
            'TAmt': [],
            'Rmng': [],
        }
        transactions = re.findall(rgx_trns, table)
        for t in transactions:
            date, desc, amt1, amt2 = t
            if amt2 == '':
                amt2 = amt1
                amt1 = '0'
            log.debug(f'date: {date}, amt1: {amt1}, amt2: {amt2}, desc: {desc}')
            tdata['Date'].append(date.strip())
            tdata['Desc'].append(desc.strip())
            tdata['TAmt'].append(amt1.strip())
            tdata['Rmng'].append(amt2.strip())
        tdf = pandas.DataFrame(tdata)
        log.debug(f'\n\n\tacct: {table_acct}\n{tdf.to_markdown(index=False)}\n')
        tdf.insert(0, 'Year', year)
        tdf['Date'] = tdf.apply(lambda row: row['Year'] + '-' + row['Date'], axis=1)
        tdf = tdf.drop(columns=['Year'])
        tbl_data.append((table_acct, tdf))

    return tbl_data


def load_into_database(config: configparser.ConfigParser, acct: str, df: pandas.DataFrame):

    column_rename = {
        'Date': 't_date',
        'Desc': 't_description',
        'TAmt': 'amount',
        'Rmng': 'remaining',
    }
    df_data = df.copy().reset_index(drop=True)
    df_data.rename(columns=column_rename, inplace=True)
    df_data.insert(0, 'acct', acct)

    df_data['amount'] = df_data['amount'].apply(lambda x: str(x).replace(',', ''))
    df_data['remaining'] = df_data['remaining'].apply(lambda x: str(x).replace(',', ''))

    db_conn = util.create_database_connection(
        host=config['chl_mysql']['host'],
        port=config['chl_mysql']['port'],
        username=config['chl_mysql']['username'],
        password=config['chl_mysql']['password'],
        database='sys',
    )

    df_data.to_sql(
        con=db_conn,
        schema='FinData',
        name='TransactionsEmn',
        if_exists='append',
        index=False,
    )


if __name__ == '__main__':
    util.create_log()
    main()
