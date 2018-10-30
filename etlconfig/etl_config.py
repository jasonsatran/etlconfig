import json
import jsonpickle


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


class TableObject():

    def __init__(self, alias_name, path_to_file):
        self._alias_name = alias_name
        self._path_to_file = path_to_file

    @staticmethod
    def decode(s):
        return jsonpickle.decode(s)

    def encode(self):
        return jsonpickle.encode(self)
