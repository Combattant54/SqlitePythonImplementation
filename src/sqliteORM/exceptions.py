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