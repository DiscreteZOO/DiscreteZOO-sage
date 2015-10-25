import errno
import os
import sqlite3
from sage.rings.integer import Integer
from sage.rings.rational import Rational
from sage.rings.real_mpfr import RealNumber
from sage.rings.real_mpfr import create_RealNumber
from db import DB
from query import Count
from query import Table
from utility import int_or_real
from zooobject import ZooObject

DBFILE = os.path.join(os.path.expanduser("~"), ".graphzoo", "graphzoo.db")

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

def makeType(t):
    if type(t) == tuple:
        t, c = t
    else:
        c = set()
    cons = "".join([" " + constraints[x] for x in c])
    if issubclass(t, ZooObject):
        return "INTEGER" + cons + " REFERENCES `%s`(`%s`)" \
                                    % (t._spec["name"], t._spec["primary_key"])
    else:
        return types[t] + cons

def makeTable(t):
    if isinstance(t, Table):
        aliases = ["(%s)" % makeTable(x["table"]) if x["alias"] is None
              else "(%s) AS `%s`" % (makeTable(x["table"]), x["alias"])
                                                            for x in t.tables]
        joins = [" %sJOIN " % ("LEFT " if x["left"] else "") for x in t.tables]
        using = ["" if len(x["by"]) == 0
                 else " USING (%s)" % ", ".join(["`%s`" % c for c in x["by"]])
                 for x in t.tables]
        return aliases[0] + "".join([joins[i] + aliases[i] + using[i]
                                     for i in range(1, len(t.tables))])
    else:
        return "`%s`" % t

def makeColumn(c):
    if c is None:
        return "*"
    elif isinstance(c, Table):
        return "`%s`.*" % c.tables[0]["alias"]
    elif isinstance(c, Count):
        return "COUNT(%s)" % makeColumn(c.column)
    else:
        return c

class SQLiteDB(DB):
    db = None

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

    def connect(self, file = DBFILE):
        try:
            os.makedirs(os.path.dirname(file))
        except OSError as ex:
            if ex.errno != errno.EEXIST:
                raise ex
        self.db = sqlite3.connect(file)
        self.db.text_factory = str
        self.db.row_factory = sqlite3.Row

    def cursor(self, **kargs):
        return self.db.cursor(**kargs)

    def commit(self, **kargs):
        self.db.commit(**kargs)

    def rollback(self, **kargs):
        self.db.rollback(**kargs)

    def init_table(self, spec, commit = False):
        self.db.execute("CREATE TABLE IF NOT EXISTS `%s` (%s)" %
                        (spec["name"], ", ".join(["`%s` %s" % (k, makeType(v))
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
                                ", ".join(["?"] * len(cols))),
                    [self.to_db_type(row[c]) for c in cols])
        if ret:
            if commit:
                self.db.commit()
            return cur
        else:
            cur.close()
            if commit is not False:
                self.db.commit()

    def query(self, columns, table, query = None, cur = None):
        t = makeTable(table)
        c = ", ".join([makeColumn(col) for col in columns])
        q = query.keys()
        if query is None or len(query) == 0:
            w = "1"
            query = {}
        else:
            w = " AND ".join(["`%s` = ?" % k for k in q])
        if cur is None:
            cur = self.db.cursor()
        cur.execute("SELECT %s FROM %s WHERE %s" % (c, t, w),
                    [self.to_db_type(query[k]) for k in q])
        return cur
