class UndefinedType:

    def __bool__(self):
        return False

    def __repr__(self):
        return 'Undefined'

Undefined = UndefinedType()
