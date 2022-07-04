import configparser
import logging
from typing import Dict

from sqlalchemy import create_engine


def create_database_connection(host, port, username, password, database):
    engine = create_engine(f'mysql://{username}:{password}@{host}:{port}/{database}')
    return engine


def create_log() -> logging.Logger:
    log = logging.getLogger('chl')
    log.setLevel(logging.DEBUG)
    logging_handler_console = logging.StreamHandler()
    logging_format = logging.Formatter('[%(filename)15s - %(lineno)4d] %(message)s')
    logging_handler_console.setFormatter(logging_format)
    log.addHandler(logging_handler_console)
    log.info("returning a new log!")
    return log


def create_aliases_map(config: configparser.ConfigParser) -> Dict[str, str]:

    assert 'aliases' in config.sections()
    aliases_map = {}
    aliases = config['aliases']
    for alias_key in aliases:
        aliases_map.update({alias_key: aliases[alias_key]})

    return aliases_map


def redact_aliases(alias_map: Dict[str, str], text: str) -> str:

    redacted_text = text
    for alias_key, alias_value in alias_map.items():
        redacted_text = redacted_text.replace(alias_key, alias_value)
    return redacted_text
