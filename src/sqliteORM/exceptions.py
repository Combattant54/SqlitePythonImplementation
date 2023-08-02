import traceback

def calling_infos(limit=3) -> traceback.FrameSummary:
    trace = traceback.extract_stack(limit=limit)
    return trace[0]

class ArgumentException(Exception):
    def __init__(self, *args: object) -> None:
        super().__init__(*args)

class UnknowPathException(ArgumentException):
    def __init__(self, *args: object) -> None:
        super().__init__(*args)

class NotCreatedTable(Exception):
    def __init__(self, *args: object) -> None:
        super().__init__(*args)

class NoParameterPassed(ArgumentException):
    def __init__(self, *args: object) -> None:
        super().__init__(*args)

class InvalidRowNameError(ArgumentException):
    def __init__(self, row_name, frame_modifier = 0) -> None:
        trace = calling_infos(limit = 3 + frame_modifier)
        msg = f"An invalid row name '{row_name}' was passed in function {trace.filename}:{trace.name}:{trace.lineno}"
        super().__init__(msg)