import inspect
from functools import partialmethod

from . import types
from . import logger_builder

logger = logger_builder.build_logger(__name__)

class RowSpecificitiesException(Exception):
    pass

class RelationalError(Exception):
    pass

class InvalidForeignKeyRelation(RelationalError):
    pass

class NoMatchingTypeRelation(RelationalError):
    pass

class Row():
    def __init__(self, 
            name, 
            type, 
            autoincrement=False, 
            unique=False, 
            primary=False, 
            nullable=False, 
            foreign_key =None
        ):
        self._name = name
        self._type = type
        self._autoincrement = autoincrement
        self._unique = unique
        self._primary = primary
        self._foreign_key = foreign_key
        self._nullable = nullable
    
    def get_row_name(self) -> str:
        return self._name
    
    def get_row_type(self) -> str:
        return self._type
    
    def is_autoincrement(self):
        return self._autoincrement
    
    def is_primary(self):
        return self._primary
    
    def is_unique(self):
        if self.is_primary():
            return True
        return self._unique

    def is_nullable(self):
        return self._nullable
    
    def get_foreign_key(self):
        return self._foreign_key

    def validate(self):
        if self.is_primary() and self.is_nullable():
            raise RowSpecificitiesException("A primary row can't be nullable")
        if not self.get_row_name():
            raise RowSpecificitiesException(f"The row name '{self.get_row_name()}' is an invalid row name.")
        if not self.get_row_type():
            raise RowSpecificitiesException(f"The row type '{self.get_row_type()}' is an invalid row type.")
        if self.get_foreign_key() is not None and self.get_foreign_key().get_row_type().lower() != self.get_row_type().lower():
            raise RowSpecificitiesException(f"Invalid foreign key of {self.get_foreign_key} with type of {self.get_row_type()}")
        
        return True
    
    def __repr__(self):
        string = "Row("
        signature = inspect.signature(self.__init__)
        for arg, param in signature.parameters.items():
            try:
                val = getattr(self, "_" + arg)
                if val != param.default:
                    string += f"{arg}={val}" + ", "
            except:
                pass
        
        string = string.strip(", ")
        string += ")"
        
        return string

class DBRow(Row):
    def __init__(self, name, type, autoincrement=False, unique=False, primary=False, nullable=False, foreign_key:Row =None):
        super().__init__(name, type, autoincrement, unique, primary, nullable, foreign_key)
        self.table = None
    
    def add_references(self, reference_row: Row):
        if reference_row.get_row_type() == self.get_row_type():
            self._references.append(reference_row)
    
    def get_row_type(self) -> str:
        if isinstance(self._type, types.SqlType):
            return self._type.as_sql()
        else:
            return str(self._type)
    
    def validate(self):
        if not super().validate():
            return False
        
        if self.get_row_type().lower() not in ["int", "integer"] and self.is_autoincrement():
            raise RowSpecificitiesException(f"An invalid type of '{self.get_row_type()}' detected for autoincrement.")
        
        return True

    def get_sql_strings(self):
        string = f"""{self.get_row_name()} {self.get_row_type()}"""
        
        if self.is_primary():
            string += " PRIMARY KEY"
        elif self.is_unique():
            string += " UNIQUE"
        elif not self.is_nullable():
            string += " NOT NULL"
        
        if self.is_autoincrement():
            string += " AUTOINCREMENT"
        
        if self.get_foreign_key() is not None:
            return string, (self.get_row_name(), self.get_foreign_key())
        
        return string, ()
    
    @staticmethod
    def build_id_row(name="id"):
        row = DBRow(name=name, type=types.INTEGER, autoincrement=True, unique=True, primary=True, nullable=False)
        return row

class Relations(Row):
    def __init__(self, name, table, match_with: dict[partialmethod[Row], partialmethod[Row]], multiple="OR"):
        super().__init__(name, type="list", autoincrement=False, unique=False, primary=False, nullable=False, foreign_key=None)
        self.match_with = match_with
        self.multiple = multiple
        
    def validate(self):
        # appelle les getter des row à matcher dans la boucle pour vérifier l'égalité des types 
        for input_getter, target_getter in self.match_with.items():
            try:
                # Vérifie que les row sont bien des instance de la class Rows
                input, target = input_getter(), target_getter()
                assert(isinstance(input, (Row)))
                assert(isinstance(target, (Row)))
                
                # Vérifie la correspondance des types
                if input.get_row_type() != target.get_row_type():
                    raise NoMatchingTypeRelation(
                        f"The types '{input.get_row_type()}' and '{target.get_row_type()}' don't match.", 
                        input, 
                        target
                    )

                if target.get_foreign_key() is None:
                    raise InvalidForeignKeyRelation("The 'Relation' row must target a row with valid foreign key")
            except Exception as e:
                # TODO à modifier pour gérer les erreurs mais plus spécialisées
                raise e
