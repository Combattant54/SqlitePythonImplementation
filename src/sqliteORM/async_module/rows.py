import sqliteORM.rows as rows
from sqliteORM.rows import Row

class AsyncDBRow(rows.DBRow):
    # def __init__(
    #         self, 
    #         name, 
    #         type, 
    #         autoincrement=False, 
    #         unique=False, 
    #         primary=False, 
    #         nullable=False, 
    #         default=None, 
    #         foreign_key: Row = None,
    #         const_value: str ="",
    #     ):
    #     super().__init__(name, type, autoincrement, unique, primary, nullable, default, foreign_key, const_value)
    
    async def get_reference(self, value, _access_id=None):
        if self.get_foreign_key() is not None:
            string = self._get_reference(value)
            if _access_id is None:
                async with self.table.db.get_lock() as (db, access_id):
                    r = await db.execute(access_id, string, tuple(value))
            else:
                r = await db.execute(_access_id, string, tuple(value))
            
            return r
        
        