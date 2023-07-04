from functools import partial
from typing import Any, Iterable, Iterator
import sqlite3
import os

from . import rows
from . import checks

class ArgumentError(Exception):
    pass

from . import logger_builder
logger = logger_builder.build_logger(__name__)

def transform_foreign(x):
    row = x[1]()
    assert isinstance(row, rows.Row)
    logger.debug(str(row))
    
    return x[0], row

class DuplicatedRowError(ArgumentError):
    def __init__(self, *args: object) -> None:
        super().__init__("Duplicated row of names ", *args)

class DBTable:
    db = None
    
    def __init__(self, **kwargs) -> None:
        super().__init__()
        self._values = kwargs.copy()
        
        already_exists = True
        for row in self.__class__.rows.keys():
            if not row in kwargs:
                already_exists = False
                break
        
        if already_exists and self.__class__.get_data(**kwargs) == kwargs:
            return
        
        try:
            string = f"INSERT INTO {type(self).__name__} ({', '.join(kwargs.keys())}) VALUES ({', '.join(['?'] * len(kwargs.values()))})"
            logger.debug(string)
            cursor = type(self).db.execute(string, kwargs.values())
            
            self._values = self.__class__.get_data(id=cursor.lastrowid)
        except sqlite3.IntegrityError:
            self._values = self.__class__.get_data(**kwargs)
        
        
    @classmethod
    def add_row(cls, row: rows.Row):
        if getattr(cls, "rows", None) is None:
            cls.rows = {}
            
        name = row.get_row_name().lower()
        if name.startswith("_"):
            raise ArgumentError(f"Row name can't start with '_' : invalid row name of {row.get_row_name()}")
        if not name in cls.rows:
            row.table = cls
            cls.rows[name] = row
        else:
            raise DuplicatedRowError(cls.rows[name], row)
    
    @classmethod
    def validate_rows(cls):
        for row in cls.rows:
            if not row.validate():
                return False
        
        return True
    
    @classmethod
    def get_string(cls):
        end_line = ", \n"
        string = f"""CREATE TABLE IF NOT EXISTS {cls.__name__.lower()} (\n"""
        foreign_dict = []
        
        logger.debug(str(cls.rows))
        
        for name, row in cls.rows.items():
            sql_strings_builder = getattr(row, "get_sql_strings", None)
            
            if sql_strings_builder is not None:
                row_string, foreign = sql_strings_builder()
                row_string += end_line
                
                if foreign:
                    foreign_dict.append(foreign)
                string += row_string
                
            else:
                logger.warning(dir(row))
        
        logger.info(foreign_dict)
        
        if foreign_dict:
            foreign_dict = list(map(transform_foreign, foreign_dict))

            foreign_by_parents = {}
            for name, row in foreign_dict:
                l = foreign_by_parents.get(row.table.__name__, [])
                l.append((name, row))
                foreign_by_parents[row.table.__name__] = l
            
            
            for parent_table, values in foreign_by_parents.items():
                if not values:
                    continue
                
                string += f"FOREIGN KEY ({', '.join(map(lambda x: x[0], values))}) "
                string += f"REFERENCES {parent_table}({', '.join(map(lambda x: checks.get_row_name(x[1]), values))} ) "
                string += "ON UPDATE CASCADE ON DELETE RESTRICT"
                string += end_line
        
        string = string.removesuffix(end_line)    
        string += ")"
        
        return string
    
    @classmethod
    def get_data(cls, **kwargs):
        args_list = []
        for key, value in kwargs.items():
            args_list.append(value)
        
        string = f"SELECT * FROM {cls.__name__} WHERE (" + " AND ".join([f"{row_name} = ?" for row_name in kwargs.keys()]) + ")"
        print(string)
        r = cls.db.execute(string, args_list)
        
        print(r, dir(r))
        value = r.fetchone()
        print(value)
        found_args = {}
        
        row_conter = 0
        for k, v in cls.rows.items():
            print(row_conter, (k, v))
            if not isinstance(v, rows.DBRow):
                continue
            
            found_args[k] = value[row_conter]
            row_conter += 1
        
        return found_args
    
    @classmethod
    def get_by(cls, **kwargs):
        data = cls.get_data(**kwargs)
        return cls(**data)
    
    @classmethod
    def _get_row(cls, name):
        return cls.rows.get(name)
    
    @classmethod
    def get_row(cls, name):
        return partial(cls._get_row, name)
    
    def values(self):
        return self._values
    
    def __repr__(self) -> str:
        string = f"{self.__class__.__name__}("
        
        for k, v in iter(self):
            string += f"{k} = {repr(v)}, "
        
        string += ")"
        
        return string

    def __iter__(self) -> Iterator:
        return iter(self.values.items())
    
    def __getattribute__(self, __name: str) -> Any:
        try:
            return self.values[__name]
        except:
            return super().__getattribute__(__name)
    
    def __getitem__(self, k):
        return self.values[k]
    
    

class DB():
    def __init__(self, 
            tables: set[DBTable] =set(), 
            path=os.path.join(os.path.dirname(__file__), "data", "db.db"), 
            debug=False
        ) -> None:
        if not os.path.exists(os.path.dirname(path)):
            os.makedirs(os.path.dirname(path))
        
        self.path = path
        print(self.path)
        self.conn = sqlite3.connect(self.path)
        self.tables = tables
        self.debug = debug
    
    def add_table(self, table: DBTable) -> None:
        table.create()
        self.tables.add(table)
        table.db = self
    
    def get_conn(self, force_new = False):
        # La connection existe déja et une nouvelle n'est pas demandée
        if (not force_new) and (self.conn is not None):
            return self.conn
        
        #cré une nouvelle connection
        try:
            self.conn = sqlite3.connect(self.path)
        except Exception as e:
            logger.exception("Error in getting connection, force = " + str(force_new))
        
        return self.conn

    def create_tables(self):
        if not self.debug:
            self.debug = True
            
        for table in self.tables:
            string = table.get_string()
            print("\n")
            print(string)
            r = self.execute(string)
            print(r)
        
        self.commit("Tables créées", force_commit=True)
    
    def add_value(self, value: DBTable):
        if not value.__class__ in self.tables:
            logger.warning(f"Table {value.__class__} not found")
        
        string = f"INSERT INTO {value.__class__} ({', '.join(value.values.keys())})\n"
        string += "VALUES ("
        string += ", ".join(value.values.items())
        string += ")"
        
        self.execute(string)
    
    def commit(self, message="", force_commit=False):
        conn = self.get_conn()
        
        # retourne si pas de changement
        if conn.total_changes <= 0 and not force_commit:
            print(conn.total_changes)
            return
        
        # cré le message de commit s'il y en a un
        if message:
            message = f"Committing for '{message}'"
        
        # Essaie de commit et debug le résultat sinon log l'erreur
        try:
            conn.commit()
            if self.debug:
                message = message.format(changes=conn.total_changes)
                print(message)
                logger.debug(message)
        except Exception as e:
            logger.error(message, exc_info=True)
    
    def execute(self, command: str, params_tuple: tuple =(), many=False, force_new=False):
        params_tuple = tuple(params_tuple)
        conn = self.get_conn(force_new)
        r = None
        
        try:
            if not many:
                r = conn.execute(command, params_tuple)
            else:
                r = conn.executemany(command, params_tuple)
        except sqlite3.IntegrityError as e:
            conn.rollback()
            if "UNIQUE constraint failed:" in str(e):
                return None
            else:
                raise e
        except sqlite3.ProgrammingError as e:
            conn.rollback()
            if "Cannot operate on a closed database." in str(e):
                r = self.execute(command, params_tuple, many, force_new=True)
            else:
                raise e
        except Exception as e:
            conn.rollback()
            print("\n")
            logger.exception("Unhandled error in execute for " + command + " with parameters " + str(params_tuple))
            print("\n")
        
        return r