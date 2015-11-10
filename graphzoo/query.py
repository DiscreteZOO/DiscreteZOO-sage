class All: pass

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

    def getColumns():
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

    def __add__(self, other):
        return Plus(self, other)

    def __radd__(self, other):
        return Plus(other, self)

    def __sub__(self, other):
        return Minus(self, other)

    def __rsub__(self, other):
        return Minus(other, self)

    def __mul__(self, other):
        return Times(self, other)

    def __rmul__(self, other):
        return Times(other, self)

    def __div__(self, other):
        return Divide(self, other)

    def __rdiv__(self, other):
        return Divide(other, self)

    def __mod__(self, other):
        return Modulo(self, other)

    def __rmod__(self, other):
        return Modulo(other, self)

    def __pow__(self, other):
        return Power(self, other)

    def __rpow__(self, other):
        return Power(other, self)

    def __lshift__(self, other):
        return LeftShift(self, other)

    def __rlshift__(self, other):
        return LeftShift(other, self)

    def __rshift__(self, other):
        return RightShift(self, other)

    def __rrshift__(self, other):
        return RightShift(other, self)

    def __and__(self, other):
        return BitwiseAnd(self, other)

    def __rand__(self, other):
        return BitwiseAnd(other, self)

    def __or__(self, other):
        return BitwiseOr(self, other)

    def __ror__(self, other):
        return BitwiseOr(other, self)

    def __xor__(self, other):
        return BitwiseXOr(self, other)

    def __rxor__(self, other):
        return BitwiseXOr(other, self)

    def __neg__(self):
        return Negate(self)

    def __pos__(self):
        return self

    def __abs__(self):
        return Absolute(self)

    def __invert__(self):
        return Invert(self)

    __floordiv__ = __div__
    __truediv__ = __div__
    __rfloordiv__ = __rdiv__
    __rtruediv__ = __rdiv__

class Value(Expression):
    value = None

    def __init__(self, value):
        self.value = value

    def getColumns():
        return set()

class Column(Expression):
    column = None
    alias = None

    def __init__(self, column, alias = None):
        self.column = column
        self.alias = alias

    def getColumns():
        if isinstance(self.column, Expression):
            return self.column.getColumns()
        else:
            return {self.column}

class BinaryOp(Expression):
    left = None
    right = None

    def __init__(self, left, right):
        self.left = makeExpression(left)
        self.right = makeExpression(right)

    def getColumns():
        return self.left.getColumns().union(self.right.getColumns())

class LessThan(BinaryOp): pass
class LessEqual(BinaryOp): pass
class Equal(BinaryOp): pass
class NotEqual(BinaryOp): pass
class GreaterThan(BinaryOp): pass
class GreaterEqual(BinaryOp): pass
class Plus(BinaryOp): pass
class Minus(BinaryOp): pass
class Times(BinaryOp): pass
class Divide(BinaryOp): pass
class Modulo(BinaryOp): pass
class Power(BinaryOp): pass
class LeftShift(BinaryOp): pass
class RightShift(BinaryOp): pass
class BitwiseAnd(BinaryOp): pass
class BitwiseOr(BinaryOp): pass
class BitwiseXOr(BinaryOp): pass
class Concatenate(BinaryOp): pass
class Is(BinaryOp): pass
class IsNot(BinaryOp): pass

class Like(BinaryOp):
    case = None

    def __init__(self, left, right, case = False):
        BinaryOp.__init__(self, left, right)
        self.case = case

class UnaryOp(Expression):
    exp = None

    def __init__(self, exp):
        self.exp = makeExpression(exp)

    def getColumns():
        return exp.getColumns()

class Not(UnaryOp): pass
class Negate(UnaryOp): pass
class Absolute(UnaryOp): pass
class Invert(UnaryOp): pass

class LogicalExpression(Expression):
    terms = None

    def __init__(self, *lterms, **kterms):
        if len(kterms) > 0:
            q = kterms.keys()
            self.__init__(*(list(lterms) + \
                    [Equal(Column(k), makeExpression(kterms[k])) for k in q]))
        else:
            self.terms = [makeExpression(e) for e in lterms]

class And(LogicalExpression): pass
class Or(LogicalExpression): pass

class Count(Expression):
    column = None

    def __init__(self, column = None, distinct = False):
        self.column = column
        self.distinct = distinct

    def getColumns():
        if isinstance(self.column, Expression):
            return self.column.getColumns()
        else:
            return {self.column}

def makeExpression(val):
    if isinstance(val, Expression):
        return val
    elif isinstance(val, basestring):
        return Column(val)
    elif type(val) == dict:
        return And(**val)
    elif type(val) in [list, set]:
        return And(*list(val))
    else:
        return Value(val)

C = Column
V = Value
