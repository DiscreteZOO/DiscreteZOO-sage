class QueryObject:
    def __repr__(self):
        return "<%s (%s) at 0x%08x>" % (self.__class__, str(self), id(self))

class All(QueryObject):
    def __str__(self):
        return "All columns"

class Table(QueryObject):
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

    def __str__(self):
        if len(self.tables) == 0:
            return 'Empty join'
        aliases = [('"%s"' % t["table"]) if t["table"] == t["alias"]
                                            or t["alias"] is None
                    else ('"%s"->"%s"' % (t["table"], t["alias"]))
                    for t in self.tables]
        return "Table %s%s" % (aliases[0], ''.join([' %sjoin %s by (%s)' %
                ("left " if t["left"] else "", aliases[i], ', '.join(t["by"]))
                for i, t in enumerate(self.tables) if i > 0]))

class Expression(QueryObject):
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
        return And(self, other)

    def __rand__(self, other):
        return And(other, self)

    def __or__(self, other):
        return Or(self, other)

    def __ror__(self, other):
        return Or(other, self)

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
        return Not(self)

    __floordiv__ = __div__
    __truediv__ = __div__
    __rfloordiv__ = __rdiv__
    __rtruediv__ = __rdiv__

class Value(Expression):
    value = None

    def __init__(self, value):
        self.value = value

    def getColumns(self):
        return set()

    def __str__(self):
        if isinstance(self.value, basestring):
            return "'%s'" % self.value
        else:
            return str(self.value)

class Column(Expression):
    column = None
    alias = None

    def __init__(self, column, alias = None):
        self.column = column
        if alias is True:
            self.alias = str(column)
        else:
            self.alias = alias

    def getColumns(self):
        if isinstance(self.column, Expression):
            return self.column.getColumns()
        else:
            return {self.column}

    def __str__(self):
        if self.alias is None:
            return '%s' % self.column
        else:
            return '%s->%s' % (self.column, self.alias)

class BinaryOp(Expression):
    left = None
    right = None
    op = None

    def __init__(self, left, right):
        self.left = makeExpression(left)
        self.right = makeExpression(right)

    def getColumns(self):
        return self.left.getColumns().union(self.right.getColumns())

    def __str__(self):
        return "(%s) %s (%s)" % (self.left, self.op, self.right)

class LessThan(BinaryOp):
    op = "<"

class LessEqual(BinaryOp):
    op = "<="

class Equal(BinaryOp):
    op = "="

class NotEqual(BinaryOp):
    op = "!="

class GreaterThan(BinaryOp):
    op = ">"

class GreaterEqual(BinaryOp):
    op = ">="

class Plus(BinaryOp):
    op = "+"

class Minus(BinaryOp):
    op = "-"

class Times(BinaryOp):
    op = "*"

class Divide(BinaryOp):
    op = "/"

class Modulo(BinaryOp):
    op = "mod"

class Power(BinaryOp):
    op = "**"

class LeftShift(BinaryOp):
    op = "<<"

class RightShift(BinaryOp):
    op = ">>"

class BitwiseAnd(BinaryOp):
    op = "&"

class BitwiseOr(BinaryOp):
    op = "|"

class BitwiseXOr(BinaryOp):
    op = "^"

class Concatenate(BinaryOp):
    op = "++"

class Is(BinaryOp):
    op = "is"

class IsNot(BinaryOp):
    op = "is not"

class Like(BinaryOp):
    op = "like"
    case = None

    def __init__(self, left, right, case = False):
        BinaryOp.__init__(self, left, right)
        self.case = case

    def __str__(self):
        out = BinaryOp.__str__(self)
        if self.case:
            out += " (case insensitive)"
        return out

class UnaryOp(Expression):
    exp = None
    op = None

    def __init__(self, exp):
        self.exp = makeExpression(exp)

    def getColumns(self):
        return exp.getColumns()

    def __str__(self):
        return "%s (%s)" % (self.op, self.exp)

class Not(UnaryOp):
    op = "not"

class Negate(UnaryOp):
    op = "-"

class Absolute(UnaryOp):
    def __str__(self):
        return "|%s|" % self.exp

class Invert(UnaryOp):
    op = "~"

class LogicalExpression(Expression):
    terms = None

    def __init__(self, *lterms, **kterms):
        if len(kterms) > 0:
            q = kterms.keys()
            self.__init__(*(list(lterms) + \
                    [Equal(Column(k), makeExpression(kterms[k])) for k in q]))
        else:
            self.terms = [makeExpression(e) for e in lterms]

    def __str__(self):
        return self.op.join("%s" % t for t in self.terms)

class And(LogicalExpression):
    op = " and "

class Or(LogicalExpression):
    op = " or "

class Count(Expression):
    column = None

    def __init__(self, column = None, distinct = False):
        self.column = column
        self.distinct = distinct

    def getColumns(self):
        if isinstance(self.column, Expression):
            return self.column.getColumns()
        else:
            return {self.column}

    def __str__(self):
        return 'Count%s (%s)' % (" distinct" if self.distinct else "",
                                 self.column)

class Order(QueryObject):
    exp = None
    order = None

    def __init__(self, exp = None):
        if isinstance(exp, Order):
            self.exp = exp.exp
            self.order = exp.order
        elif isinstance(exp, tuple):
            self.exp = makeExpression(exp[0])
            self.order = False if isinstance(exp[1], basestring) \
                                and exp[1].upper() == 'D' else exp[1]
        else:
            self.exp = makeExpression(exp)
            if self.order is None:
                self.order = True

    def __str__(self):
        return "%s order on %s" % ("Ascending" if self.order else "Descending",
                                    self.exp)

class Ascending(Order):
    order = True

class Descending(Order):
    order = False

def makeExpression(val):
    if isinstance(val, Expression):
        return val
    elif isinstance(val, basestring):
        return Column(val)
    elif isinstance(val, dict):
        return And(**val)
    elif isinstance(val, (list, set)):
        return And(*list(val))
    else:
        return Value(val)

A = All()
C = Column
V = Value
Asc = Ascending
Desc = Descending
