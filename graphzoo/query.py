class Table:
    tables = []

    def __init__(self, *args, **kargs):
        self.tables = [{"table": t,
                        "alias": t,
                        "left": False,
                        "by": set()} for t in args] \
                    + [{"table": t,
                        "alias": a,
                        "left": False,
                        "by": set()} for a, t in kargs]

    def join(self, table, by = set(), left = False, alias = None, **kargs):
        if len(kargs) == 1:
            alias, table = kargs.items()[0]
        elif len(kargs) != 0:
            raise NotImplementedError
        self.tables.append({"table": table,
                            "alias": alias,
                            "left": left,
                            "by": by})
        return self

class Expression:
    def __init__(self, *args, **kargs):
        raise NotImplementedError

    def __lt__(self, other):
        return LessThan(self, other)

    def __le__(self, other):
        return LessEqual(self, other)

    def __eq__(self, other):
        return Equal(self, other)

    def __ne__(self, other):
        return NotEqual(self, other)

    def __gt__(self, other):
        return GreaterThan(self, other)

    def __ge__(self, other):
        return GreaterEqual(self, other)

class Column(Expression):
    column = None
    alias = None

    def __init__(self, column, alias = None):
        self.column = column
        self.alias = alias

class BinaryOp(Expression):
    left = None
    right = None

    def __init__(self, left, right):
        self.left = left
        self.right = right

class LessThan(BinaryOp): pass
class LessEqual(BinaryOp): pass
class Equal(BinaryOp): pass
class NotEqual(BinaryOp): pass
class GreaterThan(BinaryOp): pass
class GreaterEqual(BinaryOp): pass

class Count(Expression):
    column = None

    def __init__(self, column = None, distinct = False):
        self.column = column
        self.distinct = distinct
