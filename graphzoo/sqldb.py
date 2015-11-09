import os
import sqlite3
import query
from sage.rings.integer import Integer
from sage.rings.rational import Rational
from sage.rings.real_mpfr import RealNumber
from sage.rings.real_mpfr import create_RealNumber
from db import DB
from utility import int_or_real
from zooobject import ZooObject

class SQLDB(DB):
    db = None
    
    data_string = None

    none_val = "NULL"

    convert_to = {
        Integer: int,
        Rational: float,
        RealNumber: float,
        str: str,
        bool: int,
        ZooObject: int
    }

    convert_from = {
        Integer: Integer,
        Rational: int_or_real,
        RealNumber: create_RealNumber,
        str: str,
        bool: bool,
        ZooObject: Integer
    }
    
    types = {
        Integer: "INTEGER",
        Rational: "REAL",
        RealNumber: "REAL",
        str: "TEXT",
        bool: "INTEGER"
    }

    constraints = {
        "autoincrement": "PRIMARY KEY AUTOINCREMENT",
        "not_null": "NOT NULL",
        "primary_key": "PRIMARY KEY",
        "unique": "UNIQUE"
    }

    binaryops = {
        query.LessThan: "<",
        query.LessEqual: "<=",
        query.Equal: "=",
        query.NotEqual: "<>",
        query.GreaterThan: ">",
        query.GreaterEqual: ">="
    }

    logicalexps = {
        query.And: (" AND ", "1"),
        query.Or: (" OR ", "0")
    }

    def makeType(self, t):
        if type(t) == tuple:
            t, c = t
        else:
            c = set()
        cons = "".join([" " + self.constraints[x] for x in c])
        if issubclass(t, ZooObject):
            return "INTEGER" + cons + " REFERENCES `%s`(`%s`)" \
                                    % (t._spec["name"], t._spec["primary_key"])
        else:
            return types[t] + cons

    def makeTable(self, t):
        if isinstance(t, query.Table):
            aliases = ["(%s)" % self.makeTable(x["table"])
                        if x["alias"] is None else "(%s) AS `%s`" %
                                    (self.makeTable(x["table"]), x["alias"])
                        for x in t.tables]
            joins = [" %sJOIN " % ("LEFT " if x["left"] else "")
                        for x in t.tables]
            using = ["" if len(x["by"]) == 0
                     else " USING (%s)" % ", ".join(["`%s`" % c
                                        for c in x["by"]]) for x in t.tables]
            return aliases[0] + "".join([joins[i] + aliases[i] + using[i]
                                         for i in range(1, len(t.tables))])
        else:
            return "`%s`" % t

    def makeExpression(self, exp, alias = False):
        if exp is None:
            return (self.none_val, [])
        elif isinstance(exp, query.All):
            return ("*", [])
        elif isinstance(exp, query.Table):
            return ("`%s`.*" % exp.tables[0]["alias"], [])
        else:
            exp = query.makeExpression(exp)
        if isinstance(exp, query.Value):
            return (self.data_string, [self.to_db_type(exp.value)])
        elif isinstance(exp, query.Column):
            if isinstance(exp.column, basestring):
                sql = "`%s`" % exp.column
                data = []
            else:
                sql, data = self.makeExpression(exp.column)
            if alias and exp.alias is not None:
                sql += " AS `%s`" % exp.alias
            return (sql, data)
        elif isinstance(exp, query.LogicalExpression):
            word, const = self.logicalexps[exp.__class__]
            if len(exp.terms) == 0:
                return (const, [])
            else:
                q = [self.makeExpression(x) for x in exp.terms]
                return (word.join(["(%s)" % x[0] for x in q]),
                        sum([x[1] for x in q], []))
        elif isinstance(exp, query.BinaryOp):
            lq, ld = self.makeExpression(exp.left)
            rq, rd = self.makeExpression(exp.right)
            return ("(%s) %s (%s)" % (lq, self.binaryops[exp.__class__], rq),
                    ld + rd)
        elif isinstance(exp, query.Count):
            sql, data = self.makeExpression(exp.column)
            if exp.distinct:
                sql = "COUNT(DISTINCT %s)" % sql
            else:
                sql = "COUNT(%s)" % sql
            return (sql, data)
        else:
            raise NotImplementedError

    def cursor(self, **kargs):
        return self.db.cursor(**kargs)

    def commit(self, **kargs):
        self.db.commit(**kargs)

    def rollback(self, **kargs):
        self.db.rollback(**kargs)

    def init_table(self, spec, commit = False):
        self.db.execute("CREATE TABLE IF NOT EXISTS `%s` (%s)" %
                        (spec["name"], ", ".join(["`%s` %s" %
                                                    (k, self.makeType(v))
                                        for k, v in spec["fields"].items()])))
        for col in spec["indices"]:
            self.db.execute("CREATE INDEX IF NOT EXISTS `idx_%s_%s` ON `%s`(`%s`)"
                            % (spec["name"], col, spec["name"], col))
        if commit:
            self.db.commit()

    def insert_row(self, table, row, cur = None, commit = None):
        cols = row.keys()
        if cur is False:
            cur = None
            ret = False
        else:
            ret = True
        if cur is None:
            cur = self.db.cursor()
        cur.execute("INSERT INTO `%s` (%s) VALUES (%s)" %
                        (table, ", ".join(["`%s`" % c for c in cols]),
                                ", ".join([self.data_string] * len(cols))),
                    [self.to_db_type(row[c]) for c in cols])
        if ret:
            if commit:
                self.db.commit()
            return cur
        else:
            cur.close()
            if commit is not False:
                self.db.commit()

    def query(self, columns, table, cond = None, groupby = None,
              orderby = None, limit = None, offset = None, cur = None):
        t = self.makeTable(table)
        cols = [self.makeExpression(col, alias = True) for col in columns]
        c = ", ".join([x[0] for x in cols])
        data = sum([x[1] for x in cols], [])
        if cond is not None:
            w, d = self.makeExpression(cond)
            w = " WHERE %s" % w
            data += d
        if groupby is None or len(groupby) == 0:
            g = ""
        else:
            if type(groupby) not in (set, list):
                groupby = [groupby]
            groups = [self.makeExpression(grp) for grp in groupby]
            g = " GROUP BY %s" % ", ".join([x[0] for x in groups])
            data += sum([x[1] for x in groups], [])
        if orderby is None or len(orderby) == 0:
            o = ""
        else:
            if type(orderby) is set:
                orderby = [(k, True) for k in orderby]
            elif type(orderby) is dict:
                orderby = orderby.items()
            if type(orderby) is tuple:
                orderby = [orderby]
            elif type(orderby) is not list:
                orderby = [(orderby, True)]
            else:
                orderby = [k if type(k) is tuple else (k, True)
                           for k in orderby]
                orderby = [(self.makeExpression(k),
                            False if isinstance(v, basestring)
                                and v[0].upper() == 'D' else v)
                            for k, v in orderby]
            o = " ORDER BY %s" % ", ".join("`%s` %s" %
                        (k, "ASC" if v else "DESC") for (k, _), v in orderby)
            data += sum([x[0][1] for x in orderby], [])
        if limit is None:
            l = ""
        else:
            l = " LIMIT %d" % limit
            if offset is not None:
                l += " OFFSET %d" % offset
        if cur is None:
            cur = self.db.cursor()
        cur.execute("SELECT %s FROM %s%s%s%s%s" % (c, t, w, g, o, l), data)
        return cur
