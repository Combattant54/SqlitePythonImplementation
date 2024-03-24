import inspect

import sqliteORM.types as types
import sqliteORM.logger_builder as logger_builder

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
            default=None,
            foreign_key =None,
            const_value=""
        ):
        self._name = name
        self._type = type
        self._autoincrement = autoincrement
        self._unique = unique
        self._primary = primary
        self._nullable = nullable
        self._default = default
        self._foreign_key = foreign_key
        self._const_value = const_value
    
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

    def get_default(self):
        return self._default
    
    def get_foreign_key(self):
        return self._foreign_key

    def set_const_value(self, const_value):
        self._const_value = const_value

    def get_const_value(self):
        return self._const_value
    
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
    def __init__(
            self, 
            name, 
            type, 
            autoincrement=False, 
            unique=False, 
            primary=False, 
            nullable=False, 
            default=None, 
            foreign_key:Row =None, 
            const_value: str =""):
        super().__init__(name, type, autoincrement, unique, primary, nullable, default, foreign_key, const_value)
        self.table: type = None
    
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
        
        if self.is_autoincrement() and not self.is_primary():
            raise RowSpecificitiesException(f"A row marked as autoincrement must be marked as primary.")
        
        return True

    def get_sql_strings(self, build_primary=True):
        string = f"""{self.get_row_name()} {self.get_row_type()}"""
        
        if self.is_unique() and not self.is_primary():
            string += " UNIQUE"
        elif not self.is_nullable():
            string += " NOT NULL"
        
        if self.is_autoincrement() and self.is_primary():
            if build_primary:
                string += " PRIMARY KEY AUTOINCREMENT"
            else:
                self.set_const_value(f"SELECT IFNULL(MAX({self.get_row_name()}) + 1, 0) FROM {self.table.__name__}")
        elif not self.is_primary() and self.get_default() is not None:
            string += " DEFAULT"
            if isinstance(self.get_default(), str):
                string += f" '{self.get_default()}'"
            else:
                string += f" {self.get_default()}"
            
        
        if self.get_foreign_key() is not None:
            return string, (self.get_row_name(), self.get_foreign_key())
        
        return string, ()
    
    def _get_reference(self):
        foreign_row = self.get_foreign_key()()
        string = f"SELECT * FROM {foreign_row.table.__class__.name} WHERE {foreign_row.get_row_name()} = ?"
        return string
    
    def get_reference(self, value):
        if self.get_foreign_key() is not None:
            string = self._get_reference()
            cursor = self.table.db.execute(string, tuple(value))

            return cursor.fetchone()
        
    @classmethod
    def build_id_row(cls, name="id"):
        row = cls(name=name, type=types.INTEGER, autoincrement=True, unique=True, primary=True, nullable=False)
        return row

class Relations(Row):
    def __init__(self, name, target_table, match_with: dict, multiple="OR"):
        super().__init__(name, type="list", autoincrement=False, unique=False, primary=False, nullable=False, foreign_key=None)
        self.match_with = match_with
        self.multiple = multiple
        self.target_table = target_table
        
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
    
    def get_values(self, **kwargs):
        values = {}
        for k, v in self.match_with.items():
            param_name = k().get_row_name()
            param_value = kwargs[v().get_row_name()]
            values[param_name] = param_value
            
        instances = self.target_table.get_all(self.multiple, **values)
        return instances
