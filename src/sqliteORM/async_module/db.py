import asyncio
from contextlib import contextmanager
import sqliteORM.db as db
from sqliteORM.db import DBTable
import sqliteORM.async_module.rows
from sqliteORM.exceptions import ArgumentException
from collections import deque
import aiosqlite
import logger_builder
import sqlite3

logger = logger_builder.build_logger(__name__)

class AsyncDBTable(db.DBTable):
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
    
    async def create_new(self):
        if self.already_exists:
            return
        await self.create_line(**self._values)
    
    @classmethod
    async def create_line(cls, **kwargs):
        try:
            string = f"INSERT INTO {cls.__name__} ({', '.join(kwargs.keys())}) VALUES ({', '.join(['?'] * len(kwargs.values()))})"
            logger.debug(string)
            cursor = await cls.db.execute(string, kwargs.values())
            
            return await cls.get_data(id=cursor.lastrowid)
        except sqlite3.IntegrityError:
            return await cls.get_data(**kwargs)
    
    @classmethod
    async def get_data(cls, **kwargs):
        args_list = []
        for key, value in kwargs.items():
            args_list.append(value)
        
        string = f"SELECT * FROM {cls.__name__} WHERE (" + " AND ".join([f"{row_name} = ?" for row_name in kwargs.keys()]) + ")"
        
        async with await cls.db as (db, access_id):
            r = await db.execute(access_id, string, args_list)
        
            print(r, dir(r))
            value = await r.fetchone()
            print(value)
            found_args = {}
        
        row_conter = 0
        for k, v in cls.rows.items():
            print(row_conter, (k, v))
            if not isinstance(v, rows.AsyncDBRow):
                continue
            
            found_args[k] = value[row_conter]
            row_conter += 1
        
        return found_args

class AsyncDB(db.DB):
    def __init__(self, tables: set[DBTable] = [], path=None, debug=False) -> None:
        if not path:
            raise ArgumentException("")
        super().__init__(tables, path, debug)
        
        self.current_access_id = 0
        self.next_access_id = 0
        self.is_locked = False
        self.queue = deque()
        self.conn = aiosqlite.connect(self.path)
    
    async def get_id(self):
        access_id = self.next_access_id
        self.next_access_id += 1
        return access_id
    
    @contextmanager()
    async def get_lock(self):
        try:
            access_id = await self.get_id()
            yield self.lock(access_id)
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
            self.conn = aiosqlite.connect(self.path)
        except Exception as e:
            logger.exception("Error in getting connection, force = " + str(force_new))
        
        return self.conn
        
    
    async def execute(self, access_id: int, command: str, params_tuple: tuple =(), many=False, force_new=False):
        assert self.current_access_id == access_id
        params_tuple = tuple(params_tuple)
        conn = await self.get_conn(force_new)
        r = None
        
        try:
            if not many:
                r = await conn.execute(command, params_tuple)
            else:
                r = await conn.executemany(command, params_tuple)
        except sqlite3.IntegrityError | aiosqlite.IntegrityError as e:
            conn.rollback()
            if "UNIQUE constraint failed:" in str(e):
                return None
            else:
                raise 
        except sqlite3.ProgrammingError | aiosqlite.ProgrammingError as e:
            conn.rollback()
            if "Cannot operate on a closed database." in str(e):
                r = self.execute(command, params_tuple, many, force_new=True)
            else:
                raise 
        except Exception as e:
            conn.rollback()
            print("\n")
            logger.exception("Unhandled error in execute for " + command + " with parameters " + str(params_tuple))
            print("\n")
        
        return r