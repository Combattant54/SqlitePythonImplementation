from .. import rows, db

def get_row_name(row : (rows.Row, str)) :
    if isinstance(row, rows.Row) :
        return row.get_row_name()
    else:
        return str(row)

def get_row_id(row : (rows.Row, str)) :
    if isinstance(row, rows.Row) :
        table = getattr(row, "table")
        if table is None:
            return row.get_row_name()
        elif issubclass(table, db.DBTable):
            return f"{table.__name__}.{row.get_row_name()}"
        else:
            return f"{table}({row.get_row_name()})"
    else:
        return str(row)