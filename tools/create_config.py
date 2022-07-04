import configparser

from pathlib import Path

path_config = Path('../config.ini')
config = configparser.ConfigParser()

config['chl_mysql'] = {}
config['chl_mysql']['host'] = ''
config['chl_mysql']['port'] = ''
config['chl_mysql']['username'] = ''
config['chl_mysql']['password'] = ''

config['aliases'] = {}

with path_config.open('w') as f:
    config.write(f)
