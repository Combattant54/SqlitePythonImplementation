from functools import partial
from typing import Any, Iterable, Iterator, Optional
import sqlite3
import os
import typing

from . import rows
from . import checks
from .exceptions import NotCreatedTable, NoParameterPassed, InvalidRowNameError

class ArgumentError(Exception):
    pass

from . import logger_builder
logger = logger_builder.build_logger(__name__)

def transform_foreign(x):
    row = x[1]()
    assert isinstance(row, rows.Row), f"The type {row.__class__} in {x} is invalid here"
    logger.debug(str(row))
    
    return x[0], row

class DuplicatedRowError(ArgumentError):
    def __init__(self, *args: object) -> None:
        super().__init__("Duplicated row of names ", *args)

class DBTable:
    db = None
    _is_created = False
    
    def __init__(self, **kwargs) -> None:
        super().__init__()
        self._values = {}
        
        for k, v in kwargs.items():
            if k.startswith("_"):
                logger.warning(f"Invalid row name of : {k}")
                continue
            
            self._values[k] = v
        
        if len(self._values) == 0:
            string_args = ', '.join([k + '=' + str(v) for k, v in kwargs.items()])
            raise NoParameterPassed(f"No valid parameters was passed to {self.__class__.__name__}({string_args})")
    
    def create_new(self):
        self._values = self.create_line(**self._values)
    
    def convert_all(self):
        for row in self.cls.rows:
            if not isinstance(row, rows.DBRow):
                continue
            
            value = row.get_reference(self.values()[row.get_row_name()])
            if value:
                self.values()[row.get_row_name()] = value
    
    @classmethod
    def _iter_rows(cls, multiple="AND", **kwargs):
        string = f"SELECT * FROM {cls.__name__}" 
        search_string = " WHERE (" \
            + f" {multiple} ".join([f"{row_name} = ?" for row_name in kwargs.keys()]) \
            + ")"
        
        if len(kwargs) > 0:
            string = string + search_string
        
        return string
    
    @classmethod
    def iter_rows(cls, multiple="AND", **kwargs):
        string = cls._iter_rows(multiple=multiple, **kwargs)
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
        string_insert = f"INSERT INTO {cls.__name__}"
        string_rows = " ("
        string_values = " VALUES ("
        
        absolute_args = {}
        
        for row_name, row in cls.rows.items():
            if not row_name in kwargs and row.get_const_value() != "":
                absolute_args[row_name] = row.get_const_value()
        
        rows = list(absolute_args.keys())
        rows.extend(kwargs.keys())
        
        values = list(["({})".format(val) for val in absolute_args.values()])
        values.extend(['?'] * len(kwargs.values()))
        
        string_rows += ", ".join(rows)
        string_values += ", ".join(values)
        
        string = string_insert + string_rows + ")" + string_values + ")"
        if cls.__name__ == "Records":
            logger.debug(string)
        
        return string, kwargs.values()
        
    
    @classmethod
    def create_line(cls, **kwargs):
        try:
            string, args_list = cls._create_line(**kwargs)
            
            cursor = cls.execute(string, args_list)

            if cls.__name__ == "Records":
                logger.debug(f"creating line of {cls.__name__} with string of {string} and args of {args_list}")
            
            values = cls.get_data(id=cursor.lastrowid)
            if values is None:
                values = cls.get_data(**kwargs)
            assert values is not None, f"values should have a value or raise an exception for id: {cursor.lastrowid} and kwargs : {str(**kwargs)}"
            return values
        except (sqlite3.IntegrityError, AssertionError) as e:
            logger.exception(f"Error during adding row : {str(e)}")
            values = cls.get_data(**kwargs)
            logger.error(f"getting data with {str(kwargs)}")
            return values
    
    @classmethod
    def add_row(cls, row: rows.Row):
        if getattr(cls, "rows", None) is None:
            cls.rows: typing.Dict[str, rows.Row] ={}
            
        name = row.get_row_name().lower()
        if name.startswith("_"):
            raise ArgumentError(f"Row name can't start with '_' : invalid row name of {row.get_row_name()}")
        if not name in cls.rows:
            row.table = cls
            cls.rows[name] = row
        else:
            raise DuplicatedRowError(cls.rows[name], row)
    
    @classmethod
    def validate_rows(cls) -> bool:
        for row in cls.rows:
            if not row.validate():
                return False
        
        return True
    
    @classmethod
    def get_string(cls) -> str:
        end_line = ", \n"
        string = f"""CREATE TABLE IF NOT EXISTS {cls.__name__.lower()} (\n"""
        foreign_dict = []
        primary_rows = []
        
        logger.debug(str(cls.rows))
        
        multiple_primaries = cls.has_multiple_primaries()
        
        for name, row in cls.rows.items():
            sql_strings_builder = getattr(row, "get_sql_strings", None)
            
            # récupère les informations et le sql du row
            if sql_strings_builder is not None:
                if row.is_primary() and multiple_primaries:
                    primary_rows.append(name)
                    if row.is_unique() and not row.is_autoincrement():
                        print(f"error : row {name} is unique")
                
                row_string, foreign = sql_strings_builder(build_primary=(not multiple_primaries))
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
    def _get_data(cls, **kwargs) -> str:
        args_list = []
        for key, value in kwargs.items():
            if not key in cls.rows:
                raise InvalidRowNameError(key, 1)
            args_list.append(value)
        
        string = f"SELECT * FROM {cls.__name__} WHERE (" + " AND ".join([f"{row_name} = ?" for row_name in kwargs.keys()]) + ")"
        return string, args_list
        
    
    @classmethod
    def get_data(cls, **kwargs) -> Optional[dict]:
        string, args_list = cls._get_data(**kwargs)
        
        r = cls.db.execute(string, args_list)
        value = r.fetchone()
        if value is None:
            return None
        
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
    def execute(cls, string, args_list) -> sqlite3.Cursor:
        return cls.db.execute(string, args_list)
    
    @classmethod
    def get_by(cls, **kwargs):
        data = cls.get_data(**kwargs)
        if data is None:
            return None
        return cls(**data)
    
    @classmethod
    def _get_row(cls, name):
        if not cls._is_created:
            raise NotCreatedTable(f"The row '{name}' may is unreachable because the table '{cls.__name__}' isn't created.")
        
        return cls.rows.get(name)
    
    @classmethod
    def get_row(cls, name):
        return partial(cls._get_row, name)
    
    @classmethod
    def has_multiple_primaries(cls, force=False):
        if not force:
            multiple = getattr(cls, "_multiple_primaries", None)
            if multiple is not None:
                return multiple
        
        primary = False
        for row in cls.rows.values():
            if not row.is_primary():
                continue
            elif primary:
                logger.warning(f"returning multiples primaries for class '{cls.__name__}'")
                setattr(cls, "_multiple_primaries", True)
                return True
            else:
                primary = True
        
        logger.warning(f"returning single primary for class '{cls.__name__}'")
        setattr(cls, "_multiple_primaries", False)
        return False
    
    def values(self):
        return self._values
    
    def __repr__(self) -> str:
        string = f"{self.__class__.__name__}("
        
        for k, v in iter(self):
            string += f"{k} = {repr(v)}, "
        
        string += ")"
        
        return string

    def __iter__(self) -> Iterator:
        if self._values is None:
            return iter({}.items())
        else:
            return iter(self._values.items())
    
    def __getattribute__(self, __name: str) -> Any:
        rows_dict = super().__getattribute__("__class__").rows
        
        try:
            if __name in rows_dict:
                value = self[__name]
            else:
                value = super().__getattribute__(__name)
        except:
            logger.warning(f"Nothing get in both of them for {__name} in {rows_dict} for values {super().__getattribute__('_values')}")
        else:
            return value
        
        if isinstance(rows_dict[__name], rows.Relations):
            self._values[__name] = rows_dict[__name].get_values(self._values)
        
        return self._values[__name]

    
    def __getitem__(self, k):
        return self._values[k]
    
    

class DB():
    def __init__(self, 
            tables: typing.Set[DBTable] =set(), 
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
        table._is_created = True
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

    def _create_tables(self):
        for table in self.tables:
            string = table.get_string()
            yield (table, string)
    
    def create_tables(self):
        for table, string in self._create_tables():
            try:
                r = self.execute(string)
            except Exception as e:
                print("an error occured during creating table " + table.__name__)
                e.with_traceback(e.__traceback__)
        
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
            logger.debug(f"no changes in commit '{message}' for changes : {conn.total_changes}")
            return
        
        # cré le message de commit s'il y en a un
        if message:
            message = f"Committing for '{message}'"
        
        # Essaie de commit et debug le résultat sinon log l'erreur
        try:
            conn.commit()
            if self.debug:
                message = message.format(changes=conn.total_changes)
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
                logger.debug(f"failed to execute '{command}' with parameters {params_tuple}")
                return None
            else:
                raise e
        except sqlite3.ProgrammingError as e:
            conn.rollback()
            if "Cannot operate on a closed database." in str(e):
                r = self.execute(command, params_tuple, many, force_new=True)
            else:
                raise e
        except ValueError as e:
            if "no active connection" in str(e):
                r = self.execute(command, params_tuple, many, force_new=True)
            else:
                raise
        except Exception as e:
            conn.rollback()
            logger.exception("Unhandled error in execute for " + command + " with parameters " + str(params_tuple))
        
        if r is None:
            logger.exception(f"None result for command '{command}' with parameters {params_tuple} due to : ")
        
        return r