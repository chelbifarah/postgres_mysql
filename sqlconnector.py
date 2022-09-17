
from abc import ABC, abstractmethod


class SqlConnector(ABC):
    def __init__(self, config, password):
        self.config = config
        self.password = password

    def unique_key(self):
        return self.config["unique_key"]

    @abstractmethod
    def connect_server(self):
        pass

    @abstractmethod
    def disconnect_server(self):
        pass

    @abstractmethod
    def getTables(self):
        pass

    @abstractmethod
    def getColumns(self, table):
        pass

    @abstractmethod
    def add_column(self, table, col):
        pass

    @abstractmethod
    def create_or_verify_table(self, table, header):
        pass

    @abstractmethod
    def insert_or_update(self, table, dic, columns):
        pass





