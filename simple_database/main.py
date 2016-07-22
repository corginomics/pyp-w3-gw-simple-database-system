from __future__ import print_function
import sys
import os
from datetime import date
from .exceptions import ValidationError

BASE_DB_FILE_PATH = '/tmp/simple_database/'
DB = {}

def create_database(db_name):
    DB[db_name] = Database(db_name)
    return DB[db_name]

def connect_database(db_name):
    if db_name in DB:
        return DB[db_name]
    else:
        return None
    
class Database():
    def __init__(self, name):
        self.name = name
        self.table_list = []
        file_path = BASE_DB_FILE_PATH + name + '/'
        if not os.path.exists(file_path):
            os.makedirs(file_path)
            self.file_location = BASE_DB_FILE_PATH
        else:
            raise ValidationError('Database with name "{}" already exists.'.format(name))
        
    def create_table(self, table_name, columns=None):
        self.table_list.append(table_name)
        setattr(self, table_name, Table(table_name, columns))
        
    def show_tables(self):
        return self.table_list

class Row:
    def __init__(self, *args):
        for name, value in args[0]:
            setattr(self, name, value)
            
class Table():
    def __init__(self, name, columns):
        self.name = name
        self.columns = columns
        self.column_names = [c['name'] for c in columns]
        self.column_types = [c['type'] for c in columns]
        self.num_columns = len(columns)
        self.rows = []
        
    def count(self):
        return len(self.rows)
    
    def insert(self, *args):
        self._validate_data(args)
        self.rows.append(Row(zip(self.column_names, args)))
        
    def _validate_data(self, args):
        types = {'int' : int,
                 'str' : str,
                'bool' : bool,
                'date' : date,
                }
        if self.num_columns != len(args):
            raise ValidationError("Invalid amount of fields.")
        for expected_type, actual, col_names in zip(self.column_types, args, self.column_names):
            if not isinstance(actual, types[expected_type]):
                given_type = str(actual.__class__).replace("<class '","").replace("'>","")
                raise ValidationError('Invalid type of field "{}": Given "{}", expected "{}" '.format(col_names,given_type,expected_type))

    def query(self, **kwargs):
        for row in self.rows:
            print (kwargs)
            if getattr(row, list(kwargs.keys())[0]) == list(kwargs.values())[0]:
                yield row
    
    def describe(self):
        return self.columns

    def all(self):
        for row in self.rows:
            yield row
        