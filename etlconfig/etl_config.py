import json
import jsonpickle
from pyspark import SparkContext
from etlconfig.app_spark_session import spark_session


class EtlConfig:

    def __init__(self, source_tables, destination, sql):
        self._source_tables = source_tables
        self._destination = destination
        self._sql = sql

    def encode(self):
        return jsonpickle.encode(self)

    @staticmethod
    def decode(s):
        return jsonpickle.decode(s)

    def run(self):
        for t in self._source_tables:
            # load_table resturns the df, but it is not necessary to return it here.  it is only necessary to register.
            t.load_table()
        df = spark_session.sql(self._sql)
        return df

    def runAndSave(self):
        df = self.run()
        df.repartition(1).write.mode("overwrite").csv(self._destination)
        return df


class TableObject():

    def __init__(self, alias_name, path_to_file):
        self._alias_name = alias_name
        self._path_to_file = path_to_file

    @staticmethod
    def decode(s):
        return jsonpickle.decode(s)

    def encode(self):
        return jsonpickle.encode(self)

    def load_table(self):
        df = spark_session.read.option(
            "header", "true").csv(self._path_to_file)
        df.createOrReplaceTempView(self._alias_name)
        return df
