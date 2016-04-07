r"""
Query objects

This module implements objects used to build query expressions.
"""

import re

class QueryObject(object):
    r"""
    A superclass for all query objects.
    """
    def __repr__(self):
        return "<%s (%s) at 0x%08x>" % (self.__class__, str(self), id(self))

class All(QueryObject):
    r"""
    The choice of all columns in a query.
    """
    def __str__(self):
        return "All columns"

class Table(QueryObject):
    r"""
    A table in the database.
    """
    tables = []
    index = 0

    def __init__(self, *args, **kargs):
        r"""
        Object constructor.

        INPUT:

        - an unnamed attribute can be either a ``Table`` object, or a string
          representing the table's name - in the latter case it will also be
          used as its alias.

        - a named attribute can take the same values as an unnamed attribute,
          but the attribute name will be used as an alias.
        """
        if len(args) == 1 and len(kargs) == 0 and isinstance(args[0], Table):
            self.tables = args[0].tables[:]
        else:
            self.tables = [{"table": t,
                            "alias": Table.alias(t),
                            "left": False,
                            "by": None} for t in args] \
                        + [{"table": t,
                            "alias": a,
                            "left": False,
                            "by": None} for a, t in kargs.items()]

    def join(self, table, by = None, left = False, alias = None, **kargs):
        r"""
        Join a table to the object.

        Add a new table to be represented by ``self``, joining it according
        to the parameters.

        INPUT:

        - ``table`` - a string or ``Table`` to join.

        - ``by`` - either a ``frozenset`` of columns to join by, or a tuple of
          pairs, where the first elements represent columns of the joined
          table, and the second elements are expressions that respective
          columns should match in value. The default value of ``None``
          represents a full join.

        - ``left`` - whether to perform a left join (default: ``False``).

        - ``alias`` - the alias for the joined table. The default value of
          ``None`` will keep the existing alias, if any, or will use the name
          as an alias.

        - any other named parameter will be used as ``table``, with the name
          of the parameter being used as ``alias``.
        """
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
        r"""
        Return a set of tables joined by ``self``.
        """
        return set(sum([list(t["table"].getTables())
                        if isinstance(t["table"], Table)
                        else [t["table"]] for t in self.tables], []))

    @staticmethod
    def alias(table = None):
        r"""
        Return a unique alias for the specified table.

        If a ``Table`` containing a single table is given, returns ``None``
        (i.e., no renaming takes place). If it contains multiple tables, a
        fresh alias is generated. If any other input is given, it is returned
        in string form. If no input is given, a fresh alias is generated, too.

        INPUT:

        - ``table`` - the table to return an alias for (default: ``None``).
        """
        if table is None:
            alias = "_table%d" % Table.index
            Table.index += 1
            return alias
        elif isinstance(table, Table):
            if len(table.tables) == 1:
                return None
            else:
                alias = "_join%d" % Table.index
                Table.index += 1
                return alias
        else:
            return str(table)

    @staticmethod
    def name(table):
        r"""
        Return the name of the specified table.

        If a ``Table`` is given, its alias is returned if set, otherwise the
        method is called recursicely on the first table. If any other input is
        given, it is returned in string form.

        INPUT:

        - ``table`` - the table to return the name for.
        """
        if table["alias"] is not None:
            return table["alias"]
        if isinstance(table["table"], Table):
            return Table.name(table["table"].tables[0])
        else:
            return str(table["table"])

    def __str__(self):
        if len(self.tables) == 0:
            return 'Empty join'
        aliases = [('(%s)' % t["table"]) if t["table"] == t["alias"]
                                            or t["alias"] is None
                    else ('(%s)->"%s"' % (t["table"], t["alias"]))
                    for t in self.tables]
        return "Table %s%s" % (aliases[0], ''.join([' %sjoin %s by (%s)' %
                ("left " if t["left"] else "", aliases[i],
                 ', '.join([('%s = %s' % (("%s.%s" % x[0])
                                          if isinstance(x[0], tuple) else x[0],
                                         x[1])) if isinstance(x, tuple) else x
                            for x in t["by"]]))
                for i, t in enumerate(self.tables) if i > 0]))

class Expression(QueryObject):
    r"""
    Abstract expression object.
    """
    def __init__(self, *args, **kargs):
        r"""
        Object constructor.

        Not implemented, to be overridden.
        """
        raise NotImplementedError

    def getTables(self):
        r"""
        Return a set of tables referenced by ``self``.

        Not implemented, to be overridden. See ``Column.getTables`` for the
        output format.
        """
        raise NotImplementedError

    def eval(self, parse):
        r"""
        Evaluate expression.

        Not implemented, to be overridden.

        INPUT:

        - ``parse`` - a callback function.
        """
        raise NotImplementedError

    def __hash__(self):
        return hash(str(self))

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

    def __floordiv__(self, other):
        return FloorDivide(self, other)

    def __rfloordiv__(self, other):
        return FloorDivide(other, self)

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

    __truediv__ = __div__
    __rtruediv__ = __rdiv__

class Value(Expression):
    r"""
    Value object.
    """
    value = None

    def __init__(self, value):
        r"""
        Object constructor.

        INPUT:

        - ``value`` - the value to be represented.
        """
        self.value = value

    def getTables(self):
        r"""
        Return a set of tables referenced by ``self``.

        Since the value does not reference anything, this method returns an
        empty set.
        """
        return set()

    def eval(self, parse):
        r"""
        Evaluate expression.

        Returns the represented value.

        INPUT:

        - ``parse`` - a callback function.
        """
        return self.value

    def __str__(self):
        if isinstance(self.value, basestring):
            return "'%s'" % self.value
        else:
            return str(self.value)

class Column(Expression):
    r"""
    Database column object.
    """
    column = None
    table = None
    colalias = None
    join = None
    by = None
    cond = None

    def __init__(self, column, table = None, alias = None, join = None,
                 by = None):
        r"""
        Object constructor.

        INPUT:

        - ``column`` - the column to be represented.

        - ``table`` - the table containing the column (default: ``None``).

        - ``alias`` - the alias of the column (default: ``None``).

        - ``join`` - the table to join (default: ``None``).

        - ``by`` - the criterion to join by (default: ``None``).
          See ``Table.join`` for more information.
        """
        self.column = column
        self.table = table
        self.join = join
        self.by = by
        if alias is True:
            self.colalias = str(column)
        else:
            self.colalias = alias

    def getTables(self):
        r"""
        Return a set of tables referenced by ``self``.

        If table information is available, it is returned as a triple
        containing the table, the joined table, and the joining criterion.
        The last two elements will be ``None`` if not set. The triple is
        returned as an element of the set.
        """
        if isinstance(self.column, Expression):
            return self.column.getTables()
        elif self.table is not None:
            return {(self.table, self.join, self.by)}
        else:
            return set()

    def getJoin(self):
        r"""
        Return a table representing the join of tables containing the column.

        If no table information is available, ``None`` is returned.
        """
        if self.table is None:
            return None
        elif self.join is None:
            return self.table
        else:
            return Table(self.join).join(self.table, by = self.by)

    def eval(self, parse):
        r"""
        Evaluate expression.

        Returns the value of the callback function on the column name.

        INPUT:

        - ``parse`` - a callback function.
        """
        return parse(self.column)

    def __str__(self):
        column = '%s' % self.column
        if self.table is not None:
            column = '%s.%s' % (self.table, column)
        if self.colalias is not None:
            column = '%s->%s' % (column, self.colalias)
        if self.join is not None:
            column = '%s joining %s by %s' % (column, self.join, self.by)
        return column

class ColumnSet(Column):
    r"""
    Column set object.
    """
    cl = None
    foreign = None
    ordering = None
    table = None
    subtables = None

    def __init__(self, cl, column = None, alias = None, join = None,
                 by = None, foreign = None, ordering = None, newcond = None):
        r"""
        Object constructor.

        INPUT:

        - ``cl`` - the class whose columns are represented, or a ``ColumnSet``
          representing a class.

        - ``column`` - the default column (default: ``None``). If specified,
          the object will behave like a ``Column`` representing ``column``.

        - ``alias`` - the alias of the column (default: ``None``).

        - ``join`` - the table to join (default: ``None``).

        - ``by`` - the criterion to join by (default: ``None``).
          See ``Table.join`` for more information.

        - ``foreign`` - the column referencing the base object
          (default: ``None``).

        - ``ordering`` - the ordering of key columns for indexing purposes
          (default: ``None``).

        - ``newcond`` - a condition imposed by indexing (default: ``None``).
        """
        self.subtables = {}
        if isinstance(cl, ColumnSet):
            self.cl = cl.cl
            self.foreign = cl.foreign
            self.ordering = cl.ordering
            self.table = Table(**{Table.alias(): cl.table})
            self.join = cl.join
            self.by = cl.by
            cond = cl.cond
            column = cl.column
            alias = cl.colalias
        else:
            self.cl = cl
            self.foreign = foreign
            self.ordering = ordering
            self.table = Table(**{Table.alias(): cl._spec["name"]})
            self.join = join
            self.by = by
        if newcond is not None:
            if self.by is None:
                self.by = []
            elif isinstance(by, frozenset):
                self.by = [(k, Column(k, table = self.join)) for k in self.by]
            self.by = tuple(self.by + newcond)
        if column is not None:
            Column.__init__(self, column = column, table = self.table,
                            alias = alias, join = self.join, by = self.by)

    def __str__(self):
        cset = "Columns of %s" % self.cl
        add = []
        if self.column is not None:
            add.append("default %s" % Column.__str__(self))
        if self.foreign is not None:
            add.append("foreign key %s" % self.foreign)
        if len(add) > 0:
            cset = "%s with %s" % (cset, " and ".join(add))
        return cset

    def __getattr__(self, name):
        cl = self.cl
        while cl is not None:
            if name in cl._spec["fields"]:
                v = cl._spec["fields"][name]
                try:
                    col = v._get_column(v, name, table = self.table,
                                        join = self.join, by = self.by)
                except AttributeError:
                    col = Column(name, table = self.table, join = self.join,
                                 by = self.by)
                self.__setattr__(name, col)
                return col
            cl = cl._parent
        raise AttributeError(name)

    def __getitem__(self, k):
        tk = tuple(enlist(k))
        if tk not in self.subtables:
            st = ColumnSet(self, newcond = tuple((c, Value(tk[i])) for i, c
                                                 in enumerate(self.ordering)))
            try:
                self.subtables[tk] = st
            except TypeError:
                pass
        else:
            st = self.subtables[tk]
        return st

class BinaryOp(Expression):
    r"""
    Generic binary expression object.
    """
    left = None
    right = None
    op = None

    def __init__(self, left, right):
        r"""
        Object constructor.

        INPUT:

        - ``left`` - the left argument.

        - ``right`` - the right argument.
        """
        self.left = makeExpression(left)
        self.right = makeExpression(right)

    def getTables(self):
        r"""
        Return a set of tables referenced by ``self``.

        See ``Column.getTables`` for the format of set elements.
        """
        return self.left.getTables().union(self.right.getTables())

    def __str__(self):
        return "(%s) %s (%s)" % (self.left, self.op, self.right)

class LessThan(BinaryOp):
    r"""
    'Less than' object.
    """
    op = "<"

    def eval(self, parse):
        r"""
        Evaluate expression.

        INPUT:

        - ``parse`` - a callback function.
        """
        return parse(self.left) < parse(self.right)

class LessEqual(BinaryOp):
    r"""
    'Less than or equal' object.
    """
    op = "<="

    def eval(self, parse):
        r"""
        Evaluate expression.

        INPUT:

        - ``parse`` - a callback function.
        """
        return parse(self.left) <= parse(self.right)

class Equal(BinaryOp):
    r"""
    Equality object.
    """
    op = "="

    def eval(self, parse):
        r"""
        Evaluate expression.

        INPUT:

        - ``parse`` - a callback function.
        """
        return parse(self.left) == parse(self.right)

class NotEqual(BinaryOp):
    r"""
    Inequality object.
    """
    op = "!="

    def eval(self, parse):
        r"""
        Evaluate expression.

        INPUT:

        - ``parse`` - a callback function.
        """
        return parse(self.left) != parse(self.right)

class GreaterThan(BinaryOp):
    r"""
    'Greater than' object.
    """
    op = ">"

    def eval(self, parse):
        r"""
        Evaluate expression.

        INPUT:

        - ``parse`` - a callback function.
        """
        return parse(self.left) > parse(self.right)

class GreaterEqual(BinaryOp):
    r"""
    'Greater than or equal' object.
    """
    op = ">="

    def eval(self, parse):
        r"""
        Evaluate expression.

        INPUT:

        - ``parse`` - a callback function.
        """
        return parse(self.left) >= parse(self.right)

class Plus(BinaryOp):
    r"""
    Addition object.
    """
    op = "+"

    def eval(self, parse):
        r"""
        Evaluate expression.

        INPUT:

        - ``parse`` - a callback function.
        """
        return parse(self.left) + parse(self.right)

class Minus(BinaryOp):
    r"""
    Subtraction object.
    """
    op = "-"

    def eval(self, parse):
        r"""
        Evaluate expression.

        INPUT:

        - ``parse`` - a callback function.
        """
        return parse(self.left) - parse(self.right)

class Times(BinaryOp):
    r"""
    Multiplication object.
    """
    op = "*"

    def eval(self, parse):
        r"""
        Evaluate expression.

        INPUT:

        - ``parse`` - a callback function.
        """
        return parse(self.left) * parse(self.right)

class Divide(BinaryOp):
    r"""
    Division object.
    """
    op = "/"

    def eval(self, parse):
        r"""
        Evaluate expression.

        INPUT:

        - ``parse`` - a callback function.
        """
        return parse(self.left) / parse(self.right)

class FloorDivide(BinaryOp):
    r"""
    Floor division object.
    """
    op = "//"

    def eval(self, parse):
        r"""
        Evaluate expression.

        INPUT:

        - ``parse`` - a callback function.
        """
        return parse(self.left) // parse(self.right)

class Modulo(BinaryOp):
    r"""
    Modulo object.
    """
    op = "mod"

    def eval(self, parse):
        r"""
        Evaluate expression.

        INPUT:

        - ``parse`` - a callback function.
        """
        return parse(self.left) % parse(self.right)

class Power(BinaryOp):
    r"""
    Power object.
    """
    op = "**"

    def eval(self, parse):
        r"""
        Evaluate expression.

        INPUT:

        - ``parse`` - a callback function.
        """
        return parse(self.left) ** parse(self.right)

class LeftShift(BinaryOp):
    r"""
    Left shift object.
    """
    op = "<<"

    def eval(self, parse):
        r"""
        Evaluate expression.

        INPUT:

        - ``parse`` - a callback function.
        """
        return parse(self.left) << parse(self.right)

class RightShift(BinaryOp):
    r"""
    Right shift object.
    """
    op = ">>"

    def eval(self, parse):
        r"""
        Evaluate expression.

        INPUT:

        - ``parse`` - a callback function.
        """
        return parse(self.left) >> parse(self.right)

class BitwiseAnd(BinaryOp):
    r"""
    Bitwise conjunction object.
    """
    op = "&"

    def eval(self, parse):
        r"""
        Evaluate expression.

        INPUT:

        - ``parse`` - a callback function.
        """
        return parse(self.left) & parse(self.right)

class BitwiseOr(BinaryOp):
    r"""
    Bitwise disjunction object.
    """
    op = "|"

    def eval(self, parse):
        r"""
        Evaluate expression.

        INPUT:

        - ``parse`` - a callback function.
        """
        return parse(self.left) | parse(self.right)

class BitwiseXOr(BinaryOp):
    r"""
    Bitwise XOR object.
    """
    op = "^"

    def eval(self, parse):
        r"""
        Evaluate expression.

        INPUT:

        - ``parse`` - a callback function.
        """
        return parse(self.left) ^ parse(self.right)

class Concatenate(BinaryOp):
    r"""
    Concatenation object.
    """
    op = "++"

    def eval(self, parse):
        r"""
        Evaluate expression.

        INPUT:

        - ``parse`` - a callback function.
        """
        return str(parse(self.left)) + str(parse(self.right))

class In(BinaryOp):
    r"""
    Inclusion object.
    """
    op = "in"

    def __init__(self, left, right):
        r"""
        Object constructor.

        INPUT:

        - ``left`` - the left argument.

        - ``right`` - the right argument.
        """
        BinaryOp.__init__(self, left, right)
        if isinstance(self.right, Column) and self.right.join is not None:
            self.right = Subquery([self.right.column],
                                  Table(self.right.table),
                                  cond = Column(self.right.by,
                                                self.right.join) ==
                                         Column(self.right.by,
                                                self.right.table))

    def eval(self, parse):
        r"""
        Evaluate expression.

        INPUT:

        - ``parse`` - a callback function.
        """
        return parse(self.left) in parse(self.right)

class Like(BinaryOp):
    r"""
    String matching object.
    """
    op = "like"
    case = None

    def __init__(self, left, right, case = False):
        r"""
        Object constructor.

        INPUT:

        - ``left`` - the left argument.

        - ``right`` - the right argument.

        - ``case`` - whether the comparison should be case-insensitive.
        """
        BinaryOp.__init__(self, left, right)
        self.case = case

    def __str__(self):
        out = BinaryOp.__str__(self)
        if self.case:
            out += " (case insensitive)"
        return out

    def eval(self, parse):
        r"""
        Evaluate expression.

        INPUT:

        - ``parse`` - a callback function.
        """
        return re.match(re.escape(parse(self.left)).replace("_", ".") \
                                                   .replace("%", ".*"),
                        parse(self.right))

class UnaryOp(Expression):
    exp = None
    op = None

    def __init__(self, exp):
        r"""
        Object constructor.

        INPUT:

        - ``exp`` - the argument.
        """
        self.exp = makeExpression(exp)

    def getTables(self):
        r"""
        Return a set of tables referenced by ``self``.

        See ``Column.getTables`` for the format of set elements.
        """
        return self.exp.getTables()

    def __str__(self):
        return "%s (%s)" % (self.op, self.exp)

class Not(UnaryOp):
    r"""
    Logical negation object.
    """
    op = "not"

    def eval(self, parse):
        r"""
        Evaluate expression.

        INPUT:

        - ``parse`` - a callback function.
        """
        return not parse(self.exp)

class Negate(UnaryOp):
    r"""
    Arithmetic negation object.
    """
    op = "-"

    def eval(self, parse):
        r"""
        Evaluate expression.

        INPUT:

        - ``parse`` - a callback function.
        """
        return -parse(self.exp)

class Absolute(UnaryOp):
    r"""
    Absolute value object.
    """
    def __str__(self):
        return "|%s|" % self.exp

    def eval(self, parse):
        r"""
        Evaluate expression.

        INPUT:

        - ``parse`` - a callback function.
        """
        return abs(parse(self.exp))

class Invert(UnaryOp):
    r"""
    Bitwise inversion object.
    """
    op = "~"

    def eval(self, parse):
        r"""
        Evaluate expression.

        INPUT:

        - ``parse`` - a callback function.
        """
        return ~parse(self.exp)

class IsNull(UnaryOp):
    r"""
    'Is null' object.
    """
    def __str__(self):
        return "%s is null" % self.exp

    def eval(self, parse):
        r"""
        Evaluate expression.

        INPUT:

        - ``parse`` - a callback function.
        """
        return parse(self.exp) is None

class IsNotNull(UnaryOp):
    r"""
    'Is not null' object.
    """
    def __str__(self):
        return "%s is not null" % self.exp

    def eval(self, parse):
        r"""
        Evaluate expression.

        INPUT:

        - ``parse`` - a callback function.
        """
        return parse(self.exp) is not None

class LogicalExpression(Expression):
    r"""
    Generic logical expression object.
    """
    terms = None

    def __init__(self, *lterms, **kterms):
        r"""
        Object constructor.

        INPUT:

        - if a single unnamed parameter is given and it is a list, set, or
          tuple, it will be interpreted as a list of expressions to join by
          the chosen logical operator.

        - otherwise, all unnamed parameters will be joined by the chosen
          logical operator.

        - any named parameter will add a term equating a column with the name
          of the parameter with the expression given by its value.
        """
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
        r"""
        Return a set of tables referenced by ``self``.

        See ``Column.getTables`` for the format of set elements.
        """
        return set(sum([list(t.getTables()) for t in self.terms], []))

class And(LogicalExpression):
    r"""
    Conjunction object.
    """
    op = " and "

    def eval(self, parse):
        r"""
        Evaluate expression.

        INPUT:

        - ``parse`` - a callback function.
        """
        return all(parse(t) for t in terms)

class Or(LogicalExpression):
    r"""
    Disjunction object.
    """
    op = " or "

    def eval(self, parse):
        r"""
        Evaluate expression.

        INPUT:

        - ``parse`` - a callback function.
        """
        return any(parse(t) for t in terms)

class Count(Expression):
    r"""
    Counting object.
    """
    column = None

    def __init__(self, column = None, distinct = False):
        r"""
        Object constructor.

        INPUT:

        - ``column`` - the column whose value should be counted. The default
          value of ``None`` specifies that all rows should be counted.

        - ``distinct`` - whether to only count distinct values
          (default: ``False``).
        """
        self.column = column
        self.distinct = distinct

    def getTables(self):
        r"""
        Return a set of tables referenced by ``self``.

        See ``Column.getTables`` for the format of set elements.
        """
        if isinstance(self.column, Expression):
            return self.column.getTables()
        else:
            return set()

    def __str__(self):
        return 'Count%s (%s)' % (" distinct" if self.distinct else "",
                                 self.column)

class Random(Expression):
    r"""
    Randomness object.
    """
    def __init__(self):
        pass

    def getTables(self):
        r"""
        Return a set of tables referenced by ``self``.

        Since randomness does not reference anything, this method returns an
        empty set.
        """
        return set()

    def __str__(self):
        return 'Random'

class Order(QueryObject):
    r"""
    Generic ordering object.
    """
    exp = None
    order = None

    def __init__(self, exp = None):
        r"""
        Object constructor.

        INPUT:

        - ``exp`` - the expression to order by (default: ``None``).
        """
        if isinstance(exp, Order):
            self.exp = exp.exp
            self.order = exp.order
        elif isinstance(exp, tuple):
            if isinstance(exp[0], basestring):
                self.exp = Column(exp[0])
            else:
                self.exp = makeExpression(exp[0])
            self.order = False if isinstance(exp[1], basestring) \
                                and exp[1].upper() == 'D' else exp[1]
        else:
            if isinstance(exp, basestring):
                self.exp = Column(exp)
            else:
                self.exp = makeExpression(exp)
            if self.order is None:
                self.order = True

    def __str__(self):
        return "%s order on %s" % ("Ascending" if self.order else "Descending",
                                    self.exp)

class Ascending(Order):
    r"""
    Ascending ordering object.
    """
    order = True

class Descending(Order):
    r"""
    Descending ordering object.
    """
    order = False

class Subquery(Expression):
    r"""
    Subquery object.
    """
    columns = None
    table = None
    cond = None
    groupby = None
    orderby = None
    limit = None
    offset = None

    def __init__(self, columns, table, cond = None, groupby = None,
                 orderby = None, limit = None, offset = None):
        r"""
        Object constructor.

        INPUT:

        - ``columns`` - the columns to be returned by the subquery.

        - ``table`` - the table to make the subquery on.

        - ``cond`` - the condition on which rows should be selected
          (default: ``None``).

        - ``groupby`` - the columns to group by (default: ``None``).

        - ``orderby`` - the columns to order by (default: ``None``).

        - ``limit`` - the maximum number of returned rows (default: ``None``).

        - ``offset`` - the number of rows to be skipped (default: ``None``).
        """
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
        r"""
        Return a set of tables referenced by ``self``.

        See ``Column.getTables`` for the format of set elements.
        """
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

def enlist(l):
    r"""
    Make a list out of ``l``.

    INPUT:

    - ``l`` - if ``l`` is a set, return a sorted list; if it is a list, return
      it; otherwise return a list containing ``l``.
    """
    if isinstance(l, set):
        l = sorted(l)
    elif not isinstance(l, list):
        l = [l]
    return l

def makeExpression(val):
    r"""
    Make an expression object.

    INPUT:

    - ``val`` - if ``val`` is a dictionary, list, set, or tuple, perform a
      conjunction of the represented expressions (see ``LogicalExpression``);
      if it is an expression, return it, otherwise wrap it as a ``Value``.
    """
    if isinstance(val, Expression):
        return val
    elif isinstance(val, dict):
        return And(**val)
    elif isinstance(val, (list, set, tuple)):
        return And(*list(val))
    else:
        return Value(val)

def makeFields(cl, module, join = None, by = None, table = None):
    r"""
    Make field objects.

    Make objects representing the fields of the class ``cl`` as members of
    ``module``. The fields of the parent class are inherited.

    INPUT:

    - ``cl`` - the class to make the objects for.

    - ``module`` - a module or object to store the field objects into.

    - ``join`` - the table to join when creating ``Column`` objects
      (default: ``None``).

    - ``by`` - the criterion to join by (default: ``None``).
      See ``Table.join`` for more information.

    - ``table`` - the table containing the columns corresponding to the
      fields (default: ``None``).
    """
    mtype = type(module)
    if cl._parent is not None:
        for k in dir(cl._parent._fields):
            if not k.startswith("_"):
                mtype.__setattr__(module, k,
                                mtype.__getattribute__(cl._parent._fields, k))
    if table is None:
        table = cl._spec["name"]
    for k, v in cl._spec["fields"].items():
        try:
            if isinstance(v, tuple):
                v = v[0]
            col = v._get_column(v, k, table = table, join = join, by = by)
        except AttributeError:
            col = Column(k, table = table, join = join, by = by)
        mtype.__setattr__(module, k, col)
    cl._fields = module

# Aliases
A = All()
R = Random()
C = Column
V = Value
Asc = Ascending
Desc = Descending
