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
        
        self.already_exists = True
        count = 0
        for row_name, row in self.__class__.rows.items():
            if not isinstance(row, rows.DBRow):
                continue
            if not row_name in kwargs:
                self.already_exists = False
                break
            else:
                count += 1
        
        if count != len(kwargs):
            self.already_exists = False
        
        if self.already_exists:
            return
    
    def create_new(self):
        if self.already_exists:
            return
        self._values = self.create_line(**self._values)
    
    def convert_all(self):
        counter = 0
        for row in self.cls.rows:
            if not isinstance(row, rows.DBRow):
                continue
            
            value = row.get_reference(self.values()[row.get_row_name()])
            if value:
                self.values()[row.get_row_name()] = value
                
    
    @classmethod
    def _iter_rows(cls):
        string = f"SELECT * FROM {cls.__name__}"
        return string
    
    @classmethod
    def iter_rows(cls):
        string = cls._iter_rows()
        cursor = cls.execute(string)
        while True:
            row = cursor.fetchone()
            if not row:
                break
            row = cls.make_instance(row)
            yield row
    
    @classmethod
    def make_instance(cls, row_value):
        found_args = {}
        row_conter = 0
        for k, v in cls.rows.items():
            if not isinstance(v, rows.DBRow):
                continue
            
            found_args[k] = row_value[row_conter]
            row_conter += 1
        
        if len(row_value) != row_conter:
            raise ValueError("Invalid row_value : " + str(row_value) + " for class " + cls.__name__)
        
        instance = cls(**found_args)
        return instance
            
    @classmethod
    def _create_line(cls, **kwargs):
        string = f"INSERT INTO {cls.__name__} ({', '.join(kwargs.keys())}) VALUES ({', '.join(['?'] * len(kwargs.values()))})"
        logger.debug(string)
        return string, kwargs.values()
        
    
    @classmethod
    def create_line(cls, **kwargs):
        try:
            string, args_list = cls._create_line(**kwargs)
            
            cursor = cls.db.execute(string, args_list)
            
            return cls.get_data(id=cursor.lastrowid)
        except sqlite3.IntegrityError:
            return cls.get_data(**kwargs)
    
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
        primary_rows = []
        
        logger.debug(str(cls.rows))
        
        for name, row in cls.rows.items():
            sql_strings_builder = getattr(row, "get_sql_strings", None)
            
            # récupère les informations et le sql du row
            if sql_strings_builder is not None:
                if row.is_primary() and not row.is_autoincrement():
                    primary_rows.append(name)
                
                row_string, foreign = sql_strings_builder()
                row_string += end_line
                
                if foreign:
                    foreign_dict.append(foreign)
                string += row_string
                
            else:
                logger.warning(dir(row))
        
        if primary_rows:
            string += f"PRIMARY KEY({', '.join(primary_rows)})" + end_line
        
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
    def _get_data(cls, **kwargs):
        args_list = []
        for key, value in kwargs.items():
            args_list.append(value)
        
        string = f"SELECT * FROM {cls.__name__} WHERE (" + " AND ".join([f"{row_name} = ?" for row_name in kwargs.keys()]) + ")"
        return string, args_list
        
    
    @classmethod
    def get_data(cls, **kwargs):
        string, args_list = cls._get_data(**kwargs)
        
        r = cls.db.execute(string, args_list)
        value = r.fetchone()
        
        found_args = {}
        
        row_conter = 0
        for k, v in cls.rows.items():
            if not isinstance(v, rows.DBRow):
                continue
            
            found_args[k] = value[row_conter]
            row_conter += 1
        
        return found_args
    
    @classmethod
    def _get_all(cls, multiple, **kwargs):
        string = f"SELECT * FROM {cls.__name__} WHERE (" \
            + f" {multiple} ".join([f"{row_name} = ?" for row_name in kwargs.keys()]) \
            + ")"
        return string, kwargs.values()
        
    
    @classmethod
    def get_all(cls, multiple="AND", **kwargs):
        string, args_list = cls._get_all(multiple, **kwargs)
        
        cursor = cls.execute(string, args_list)
        instances = []
        for args in cursor.fetchall():
            instances.append(cls.make_instance(args))
        
        return instances
            
    @classmethod
    def execute(cls, string, args_list):
        return cls.db.execute(string, args_list)
    
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
        rows_dict = super().__getattribute__("__class__").rows
        if __name in rows_dict:
            if isinstance(rows_dict[__name], rows.Relations):
                self._values[__name] = rows_dict[__name].get_values(self._values)

            return self._values[__name]
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
        self.conn = sqlite3.connect(self.path)
        self.tables = set(tables)
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