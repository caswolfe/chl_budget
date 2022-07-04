import configparser
import unittest
from pathlib import Path

from src import util


class AliasChecks(unittest.TestCase):

    def test_alias_in_files(self):

        path_root = Path('../')
        config = configparser.ConfigParser()
        config.read(Path('../config.ini'))
        alias_map = util.create_aliases_map(config)

        for filepath in path_root.rglob('*'):

            print(f'checking filepath: "{filepath.resolve()}"')
            for alias_key in alias_map.keys():
                self.assertTrue(
                    alias_key not in str(filepath.resolve()),
                    f'Found alias key value "{alias_key}"\n\tin filepath "{filepath.resolve()}"'
                )

            if filepath.is_dir() or 'config.ini' in filepath.name:
                continue

            print(f'\treading...')
            try:
                with filepath.open() as file:
                    lines = file.readlines()
                    for i in range(len(lines)):
                        for alias_key in alias_map.keys():
                            self.assertTrue(
                                alias_key not in lines[i],
                                f'Found alias key value "{alias_key}"\n\tin "{filepath.resolve()}"\n\ton line {i+1}'
                            )
            except UnicodeDecodeError:
                print(f'\tcant read this file...')


if __name__ == '__main__':
    unittest.main()
