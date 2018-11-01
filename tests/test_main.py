import unittest
import json
from etlconfig.etl_config import EtlConfig, TableObject
import os


path_to_this_file = os.path.realpath(__file__)

relative_path_to_census_file = "../resources/2010_Census_Populations_by_Zip_Code.csv"
more_data = "../resources/more_data.csv"
selected_zip = "../resources/selected_zip_code.csv"

census_file_path = os.path.join(
    path_to_this_file, relative_path_to_census_file)

more_data_path = os.path.join(
    path_to_this_file, more_data)

selected_zip_path = os.path.join(
    path_to_this_file, selected_zip)


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

    def test_it_can_load_a_file(self):
        t = TableObject(
            "t", census_file_path)
        df = t.load_table()
        self.assertEqual(df.count(), 319)


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

    def get_test_etl_config(self):
        t1 = TableObject("census", census_file_path)
        t2 = TableObject("more_data", more_data_path)
        t3 = TableObject("special_zip", selected_zip_path)
        destination_path = "/tmp/output/"
        sql = """
        select m.more_data,c.`Total Households`
        from census c
        inner join more_data m on m.zip = c.zip
        inner join special_zip s on s.zip = c.zip
        """
        etl = EtlConfig(
            [t1, t2, t3], destination_path, sql)

        return etl

    def test_it_runs_sql(self):
        etl = self.get_test_etl_config()
        result = etl.run()
        result.show()
        self.assertEqual(result.count(), 3)

    def test_run_and_save(self):

        etl = self.get_test_etl_config()
        output_path = etl._destination
        # todo:  if path exists delete it
        # if os.path.exists(output_path):
        #     os.remove(output_path)
        etl.runAndSave()
        exits = os.path.exists(output_path)
        self.assertEqual(True, exits)
