class QueryObject:
    def __repr__(self):
        return "<%s (%s) at 0x%08x>" % (self.__class__, str(self), id(self))

class All(QueryObject):
    def __str__(self):
        return "All columns"

class Table(QueryObject):
    tables = []
    index = 0

    def __init__(self, *args, **kargs):
        if len(args) == 1 and isinstance(args[0], Table):
            self.tables = args[0].tables[:]
        else:
            self.tables = [{"table": t,
                            "alias": Table.alias(t),
                            "left": False,
                            "by": set()} for t in args] \
                        + [{"table": t,
                            "alias": a,
                            "left": False,
                            "by": set()} for a, t in kargs.items()]

    def join(self, table, by = set(), left = False, alias = None, **kargs):
        if len(kargs) == 1:
            alias, table = kargs.items()[0]
        elif len(kargs) != 0:
            raise NotImplementedError
        else:
            alias = Table.alias(table)
        self.tables.append({"table": table,
                            "alias": alias,
                            "left": left,
                            "by": by})
        return self

    def getTables(self):
        return set(sum([list(t["table"].getTables())
                        if isinstance(t["table"], Table)
                        else [t["table"]] for t in self.tables], []))

    @staticmethod
    def alias(table):
        if isinstance(table, Table):
            if len(table.tables) == 1:
                return None
            else:
                alias = "_join%d" % Table.index
                Table.index += 1
        else:
            return str(table)

    @staticmethod
    def name(x):
        if x["alias"] is not None:
            return x["alias"]
        if isinstance(x["table"], Table):
            return Table.name(x["table"].tables[0])
        else:
            return str(x["table"])

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

    def getTables(self):
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

    def getTables(self):
        return set()

    def __str__(self):
        if isinstance(self.value, basestring):
            return "'%s'" % self.value
        else:
            return str(self.value)

class Column(Expression):
    column = None
    table = None
    colalias = None
    join = None
    by = None

    def __init__(self, column, table = None, alias = None, join = None,
                 by = None):
        self.column = column
        self.table = table
        self.join = join
        self.by = by
        if alias is True:
            self.alias = str(column)
        else:
            self.alias = alias

    def getTables(self):
        if isinstance(self.column, Expression):
            return self.column.getTables()
        elif self.table is not None:
            return {(self.table, self.join, self.by)}
        else:
            return set()

    def __str__(self):
        column = '%s' % self.column
        if self.table is not None:
            column = '%s.%s' % (self.table, column)
        if self.alias is not None:
            column = '%s->%s' % (column, self.alias)
        if self.join is not None:
            column = '%s joining %s by %s' % (column, self.join, self.by)
        return column

class ColumnSet(Column):
    cl = None

    def __init__(self, cl, column = None, alias = None, join = None,
                 by = None):
        self.cl = cl
        makeFields(cl, self, join = join, by = by)
        if column is not None:
            Column.__init__(self, column = column, table = cl._spec["name"],
                            alias = alias, join = join, by = by)

    def __str__(self):
        cset = "Columns of %s" % self.cl
        if self.column is not None:
            cset = "%s with default %s" % (cset, Column.__str__(self))
        return cset

class BinaryOp(Expression):
    left = None
    right = None
    op = None

    def __init__(self, left, right):
        self.left = makeExpression(left)
        self.right = makeExpression(right)

    def getTables(self):
        return self.left.getTables().union(self.right.getTables())

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

class In(BinaryOp):
    op = "in"

    def __init__(self, left, right):
        BinaryOp.__init__(self, left, right)
        if isinstance(self.right, Column) and self.right.join is not None:
            self.right = Subquery([self.right.column], Table(self.right.table),
                                  cond = Column(self.right.by, self.right.join)
                                    == Column(self.right.by, self.right.table))

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

    def getTables(self):
        return exp.getTables()

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

class IsNull(UnaryOp):
    def __str__(self):
        return "%s is null" % self.exp

class IsNotNull(UnaryOp):
    def __str__(self):
        return "%s is not null" % self.exp

class LogicalExpression(Expression):
    terms = None

    def __init__(self, *lterms, **kterms):
        if len(lterms) == 1 and isinstance(lterms[0], (list, set, tuple)):
            lterms = lterms[0]
        if len(kterms) > 0:
            q = kterms.keys()
            self.__init__(*(list(lterms) + \
                    [Equal(Column(k), makeExpression(kterms[k])) for k in q]))
        else:
            self.terms = [makeExpression(e) for e in lterms]

    def __str__(self):
        return self.op.join("%s" % t for t in self.terms)

    def getTables(self):
        return set(sum([list(t.getTables()) for t in self.terms], []))

class And(LogicalExpression):
    op = " and "

class Or(LogicalExpression):
    op = " or "

class Count(Expression):
    column = None

    def __init__(self, column = None, distinct = False):
        self.column = column
        self.distinct = distinct

    def getTables(self):
        if isinstance(self.column, Expression):
            return self.column.getTables()
        else:
            return set()

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

class Subquery(Expression):
    columns = None
    table = None
    cond = None
    groupby = None
    orderby = None
    limit = None
    offset = None

    def __init__(self, columns, table, cond = None, groupby = None,
                 orderby = None, limit = None, offset = None):
        self.columns = columns
        self.table = table
        self.cond = cond
        self.groupby = groupby
        self.orderby = orderby
        self.limit = limit
        self.offset = offset

    def __str__(self):
        out = "Columns %s from table %s" % (self.columns, self.table)
        if self.cond is not None:
            out = "%s where (%s)" % (out, self.cond)
        if self.groupby is not None:
            out = "%s grouped by %s" % (out, self.groupby)
        if self.orderby is not None:
            out = "%s ordered by %s" % (out, self.orderby)
        if self.limit is not None:
            out = "%s with limit %d" % (out, self.limit)
        if self.offset is not None:
            out = "%s offset by %d" % (out, self.offset)
        return out

    def getTables(self):
        t = self.table.getTables()
        exptables = {col.getTables() for col in self.columns}
        exptables = {tbl if isinstance(tbl, tuple) else (tbl, None, None)
                     for tbl in exptables}
        if self.cond is not None:
            exptables.update(self.cond.getTables())
        if self.groupby is not None:
            exptables.update(self.groupby.getTables())
        if self.orderby is not None:
            exptables.update(self.orderby.getTables())
        return {(table, join, by) for table, join, by in exptables
                if table not in t}

def makeExpression(val):
    if isinstance(val, Expression):
        return val
    elif isinstance(val, basestring):
        return Column(val)
    elif isinstance(val, dict):
        return And(**val)
    elif isinstance(val, (list, set, tuple)):
        return And(*list(val))
    else:
        return Value(val)

def makeFields(cl, module, join = None, by = None):
    mtype = type(module)
    if cl._parent is not None:
        for k in dir(cl._parent._fields):
            if not k.startswith("_"):
                mtype.__setattr__(module, k,
                                mtype.__getattribute__(cl._parent._fields, k))
    for k, v in cl._spec["fields"].items():
        try:
            if isinstance(v, tuple):
                v = v[0]
            col = v._get_column(v, k, table = cl._spec["name"], join = join,
                                by = by)
        except AttributeError:
            col = Column(k, table = cl._spec["name"], join = join, by = by)
        mtype.__setattr__(module, k, col)
    cl._fields = module

A = All()
C = Column
V = Value
Asc = Ascending
Desc = Descending
