import sqliteORM.rows as rows
from sqliteORM.rows import Row

class AsyncDBRow(rows.DBRow):
    def __init__(
            self, 
            name, 
            type, 
            autoincrement=False, 
            unique=False, 
            primary=False, 
            nullable=False, 
            default=None, 
            foreign_key: Row = None
        ):
        super().__init__(name, type, autoincrement, unique, primary, nullable, default, foreign_key)
        
        