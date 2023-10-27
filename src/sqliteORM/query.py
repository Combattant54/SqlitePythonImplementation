import enum
from typing import Any

from . import rows
from . import logger_builder
import checks
import typing

logger = logger_builder.build_logger(__name__)

class QueryComparaisonType(enum.Enum):
    EQUAL = 0
    NOT_EQUAL = 1
    LESS_THAN = 2
    LESS_THAN_OR_EQUAL = 3
    MORE_THAN = 4
    MORE_THAN_OR_EQUAL_TO = 5
    _values = [
        " = ?",
        " != ?"
        " < ?",
        " <= ?",
        " > ?",
        " >= ?",
    ]
    
    @staticmethod
    def get(type):
        return QueryComparaisonType._values[type]
    
class SearchCondition:
    def __init__(self, first_value: (rows.Row, Any, None)) -> None:
        pass

class Query():
    def __init__(
            self,
            table: (type, str),
            columns_filter: typing.List[tuple[(rows.Row, str, QueryComparaisonType)]] =[], 
            to_select: typing.List[(rows.Row, str)] =[],
            order_by: (rows.Row, str) =None, 
            ascending=False
        ):
        self.table = table.__name__.lower() if isinstance(table, type) else table
        self.to_select = to_select
        self.columns_filter = columns_filter
        self.order_by = order_by
        self.ascending = ascending
        self.query = None
        
    def build_query(self) -> str:
        string = "SELECT "
        if self.to_select:
            string += ", ".join(map(checks.get_row_name, self.to_select))
        else:
            string += "*"
        string += f" FROM {self.table} "
        string += " WHERE "
        
        for row, type in self.columns_filter:
            if isinstance(row, rows.Row):
                string += f"{row.checks.get_row_name()} {QueryComparaisonType.get(type)} ?, \n"

        return string 
    
    def get_query(self):
        if not self.query:
            self.query = self.build_query()
        
        return self.query


class SimpleQuery(Query):
    def __init__(
        self, 
        table: (type, str, Query), 
        columns_filter: typing.List[tuple[(rows.Row, str, QueryComparaisonType)]] = [], 
        to_select: typing.List[(rows.Row, str)] = [], 
        order_by: typing.List[(rows.Row, str)] = [], 
        ascending=False
    ):
        super().__init__(table, columns_filter, to_select, order_by, ascending)
    
    def build_query(self) -> str:
        string = "SELECT "
        
        
        # construit la liste des colonnes à sélectionner ou toutes par défaut
        if self.to_select:
            string += ", ".join(map(checks.get_row_name, self.to_select))
        else:
            string += "*"
        
        
        # Construit la table dans laquelle effectuer la recherche 
        if isinstance(self.table, Query):
            string += f" FROM ({self.table.get_query()}) "
        else:
            string += f" FROM {self.table} "
        
        
        # construit les paramètres de recherches basique
        if self.columns_filter:
            string += " WHERE "
        
            for row, type in self.columns_filter:
                string += f"{checks.get_row_name(row)} {QueryComparaisonType.get(type)} ?, \n"
            
            string.removesuffix(", \n")
        

        # ordonne les résultat
        if self.order_by:
            string += " ORDER BY "
            string += ", ".join(map(checks.get_row_name, self.order_by))
        
        
        logger.info(f"Simple query build : '{string}'")
        logger.info(f"Query build : '{super().build_query()}'")
        
        return string
    

            