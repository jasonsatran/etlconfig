import unittest
import json
from etlconfig.etl_config import EtlConfig, TableObject


class TestTableObject(unittest.TestCase):

    def test_table_object_loads_from_json(self):
        t = TableObject("alias1", "/path/to/file")

        x = t.encode()
        expected = '{"py/object": "etlconfig.etl_config.TableObject", "_alias_name": "alias1", "_path_to_file": "/path/to/file"}'
        self.assertEqual(x, expected)

    def test_table_loads_from_json(self):
        input = '{"py/object": "etlconfig.etl_config.TableObject", "_alias_name": "alias1", "_path_to_file": "/path/to/file"}'
        x = TableObject.decode(input)
        self.assertEqual(x._alias_name, "alias1")
        self.assertEqual(x._path_to_file, "/path/to/file")


class TestEtlConfig(unittest.TestCase):

    def test_encode(self):
        t1 = TableObject("alias1", "/path/x/")
        t2 = TableObject("alias2", "/other/y/")
        source_tables = [t1, t2]
        etl_config = EtlConfig(source_tables, "/dest/path/", "select 1")
        encoded = etl_config.encode()
        expected = '{"py/object": "etlconfig.etl_config.EtlConfig", "_destination": "/dest/path/", "_source_tables": [{"py/object": "etlconfig.etl_config.TableObject", "_alias_name": "alias1", "_path_to_file": "/path/x/"}, {"py/object": "etlconfig.etl_config.TableObject", "_alias_name": "alias2", "_path_to_file": "/other/y/"}], "_sql": "select 1"}'
        self.assertEqual(encoded, expected)

    def test_decode(self):
        input = '{"py/object": "etlconfig.etl_config.EtlConfig", "_destination": "/dest/path/", "_source_tables": [{"py/object": "etlconfig.etl_config.TableObject", "_alias_name": "alias1", "_path_to_file": "/path/x/"}, {"py/object": "etlconfig.etl_config.TableObject", "_alias_name": "alias2", "_path_to_file": "/other/y/"}], "_sql": "select 1"}'
        o = EtlConfig.decode(input)
        self.assertEqual(o._source_tables[1]._path_to_file, "/other/y/")
        self.assertEqual(o._sql, "select 1")
