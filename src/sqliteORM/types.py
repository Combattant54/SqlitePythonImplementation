import dataclasses
from functools import partial

from . import logger_builder
logger = logger_builder.build_logger(__name__)

classics = [
    "INTEGER",
    "BLOB",
    "TINYINT",
    "SMALLINT",
    "LONG",
    "BOOLEAN",
    "DATETIME"
]

parameterized = [
    "TEXT",
    "BIT"
]

@dataclasses.dataclass
class SqlType:
    sql: str
    def __init__(self, sql):
        self.sql = sql
    
    def as_sql(self):
        return self.sql

class ParameterSqlType(SqlType):
    param: int
    def __init__(self, sql, param=None):
        super().__init__(sql)
        self.param = param
    
    def as_sql(self):
        if self.param is None:
            return super().as_sql()
        return f"{self.sql}({self.param})"
    
    @staticmethod
    def build_class(sql, param):
        return ParameterSqlType(sql, param)


for value in classics:
    value = value.upper()
    
    # si la variable n'existe pas, cré cette varibale dans le scope local
    if locals().get(value) is None:
        
        # si la classe pour la variable n'existe pas, cré la classe dynamiquement qui n'accepte pas de paramètre  
        # déclare la classe comme une class de donnée avec le dataclass pour l'implémentation rapide des fonctions de base
        value_class = SqlType(value)
        
        locals()[value] = value_class

for value in parameterized:
    value = value.upper()
    
    if locals().get(value) is None:
        value_class = partial(ParameterSqlType.build_class, value)
        
        locals()[value] = value_class
        
DATE = TEXT(10)