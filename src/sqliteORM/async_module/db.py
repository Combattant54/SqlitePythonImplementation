import asyncio
from contextlib import asynccontextmanager
from collections import deque
import aiosqlite
import sqlite3
import traceback
import typing

import sqliteORM.db as db
import sqliteORM.async_module.rows as rows
from sqliteORM.exceptions import ArgumentException
import sqliteORM.logger_builder as logger_builder

logger = logger_builder.build_logger(__name__)

class AsyncDBTable(db.DBTable):
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
    
    async def create_new(self, _access_id=None):
        logger.warning(self._values)
        self._values = await self.create_line(_access_id=_access_id, **self._values)
        logger.warning(self._values)
        return True
    
    async def convert_all(self, _access_id=None):
        for row in self.cls.rows:
            if not isinstance(row, rows.AsyncDBRow):
                continue
            
            value = await row.get_reference(self.values()[row.get_row_name()], _access_id=_access_id)
            if value:
                self.values()[row.get_row_name()] = value
    
    @classmethod
    async def create_line(cls, _access_id=None,  **kwargs):
        try:
            string = f"INSERT INTO {cls.__name__} ({', '.join(kwargs.keys())}) VALUES ({', '.join(['?'] * len(kwargs.values()))})"
            logger.debug(string)
            cursor = await cls.db.execute(_access_id, string, kwargs.values())
            
            if cursor is None:
                return 
            primaries = {}
            for row_name, row in cls.rows.items():
                assert isinstance(row, rows.AsyncDBRow)
                if row.is_autoincrement():
                    primaries[row_name] = cursor.lastrowid
                elif row.is_primary() and row_name in kwargs:
                    primaries[row_name] = kwargs[row_name]
                    
            
            return await cls.get_data(_access_id=_access_id, **primaries)
        except sqlite3.IntegrityError:
            return await cls.get_data(_access_id=_access_id, **kwargs)
    
    @classmethod
    async def get_data(cls, _access_id=None, **kwargs):
        args_list = []
        for key, value in kwargs.items():
            args_list.append(value)
        
        string = f"SELECT * FROM {cls.__name__} WHERE (" + " AND ".join([f"{row_name} = ?" for row_name in kwargs.keys()]) + ")"
        logger.debug(string)
        if _access_id is None:
            async with await cls.db as (db, access_id):
                r = await db.execute(access_id, string, args_list)
        else:
            r = await cls.db.execute(_access_id, string, args_list)
        
        value = await r.fetchone()
        if value is None:
            return None
        
        logger.debug(value)
        found_args = {}
        
        row_conter = 0
        for k, v in cls.rows.items():
            if not isinstance(v, rows.AsyncDBRow):
                continue
            
            found_args[k] = value[row_conter]
            row_conter += 1
        
        return found_args
    
    @classmethod
    async def iter_rows(cls, _access_id=None):
        string = cls._iter_rows()
        cursor = await cls.execute(string, _access_id=_access_id)
        while True:
            row = await cursor.fetchone()
            if row is None:
                break
            row = cls.make_instance(row)
            yield row
    
    @classmethod
    async def execute(cls, string, *args, _access_id=None):
        if access_id is None:
            async with cls.db.get_lock() as (db, access_id):
                cursor = await db.execute(access_id, string, tuple(args))
        else:
            cursor = await cls.db.execute(_access_id, string, tuple(args))
        return cursor
    
    @classmethod
    async def get_all(cls, multiple="AND", _access_id=None, **kwargs):
        string, args_list = cls._get_all(multiple, **kwargs)
        
        cursor = await cls.execute(string, *args_list, _access_id=_access_id)
        instances = []
        for args in await cursor.fetchall():
            instances.append(cls.make_instance(args))
        
        return instances

    @classmethod
    async def get_by(cls, _access_id=None, **kwargs):
        data = await cls.get_data(_access_id=_access_id, **kwargs)
        if data is None:
            return None
        return cls(**data)


class AsyncDB(db.DB):
    def __init__(self, tables: typing.Set[db.DBTable] =set(), path=None, debug=False) -> None:
        if not path:
            raise ArgumentException("")
        super().__init__(tables, path, debug)
        
        self.current_access_id = 0
        self.next_access_id = 0
        self.is_locked = False
        self.queue = deque()
        self.conn = aiosqlite.connect(self.path)
    
    async def create_tables(self):
        
        async with self.get_lock() as (_, access_id):
            for table, string in self._create_tables():
                try:
                    r = await self.execute(string)
                except Exception as e:
                    e.with_traceback(e.__traceback__)
                    print("an error occured during creating table " + table.__name__)
            
            await self.commit("Tables créées", force_commit=True)
    
    async def get_id(self):
        access_id = self.next_access_id
        self.next_access_id += 1
        return access_id
    
    @asynccontextmanager
    async def get_lock(self):
        try:
            access_id = await self.get_id()
            yield await self.lock(access_id)
        finally:
            await self.release()
    
    async def lock(self, access_id):
        if not self.is_locked:
            self.current_access_id = access_id
            self.is_locked = True
            return (self, access_id)
        
        self.queue.append(access_id)
        while self.current_access_id != access_id:
            await asyncio.sleep(0)
        
        return (self, access_id)
    
    async def release(self):
        try:
            self.current_access_id = await self.queue.popleft()
        except:
            self.is_locked = False
            
    async def get_conn(self, force_new = False):
        # La connection existe déja et une nouvelle n'est pas demandée
        if (not force_new) and (self.conn is not None):
            return self.conn
        
        #cré une nouvelle connection
        try:
            print(self.path)
            self.conn = await aiosqlite.connect(self.path)
            print(self.conn)
        except Exception as e:
            logger.exception("Error in getting connection, force = " + str(force_new))
        
        return self.conn
        
    
    async def execute(self, _access_id: int, command: str, params_tuple: tuple =(), many=False, force_new=False):
        assert self.current_access_id == _access_id
        params_tuple = tuple(params_tuple)
        conn = await self.get_conn(force_new)
        r = None
        
        msg = f"Executing {command} with parameters {params_tuple}, many: {many}, force: {force_new}"
        logger.debug(msg)
        try:
            if not many:
                r = await conn.execute(command, params_tuple)
            else:
                r = await conn.executemany(command, params_tuple)
        except (sqlite3.IntegrityError, aiosqlite.IntegrityError) as e:
            await conn.rollback()
            if "UNIQUE constraint failed:" in str(e):
                msg = f"warning in command {command} : " + str(e)
                logger.warning(msg)
                print(msg)
                r = None
            else:
                raise 
        except (sqlite3.ProgrammingError, aiosqlite.ProgrammingError) as e:
            await conn.rollback()
            if "Cannot operate on a closed database." in str(e):
                r = await self.execute(_access_id, command, params_tuple, many, force_new=True)
            else:
                raise 
        except ValueError as e:
            if "no active connection" in str(e):
                r = await self.execute(_access_id, command, params_tuple, many, force_new=True)
            else:
                raise
        except Exception as e:
            await conn.rollback()
            msg = "Unhandled error in execute for " + command + " with parameters " + str(params_tuple)
            print("\n" + msg + "\n")
            logger.exception(msg)
        
        return r

    async def commit(self, message="", force_commit=False):
        conn = await self.get_conn()
        
        try:
            # retourne si pas de changement
            if conn.total_changes <= 0 and not force_commit:
                logger.debug(f"no changes in commit '{message}' for changes : {conn.total_changes}")
                return
        except ValueError:
            conn = await self.get_conn(force_new=True)
        
        # cré le message de commit s'il y en a un
        if message:
            message = f"Committing for '{message}'"
        
        # Essaie de commit et debug le résultat sinon log l'erreur
        try:
            await conn.commit()
            if self.debug:
                message = message.format(changes=conn.total_changes)
                logger.debug(message)
        except Exception as e:
            logger.error(message, exc_info=True)