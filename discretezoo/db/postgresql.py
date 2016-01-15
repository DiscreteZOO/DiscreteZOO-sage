import psycopg2, psycopg2.extensions, psycopg2.extras
from types import ModuleType
from .query import And
from .query import BitwiseXOr
from .query import Like
from .query import Or
from .query import Power
from .query import enlist
from .sqldb import SQLDB
from ..util.utility import lookup

class PostgreSQLDB(SQLDB):
    data_string = '%s'
    ident_quote = '"'
    exceptions = psycopg2.Error

    logicalconsts = {
        And: 'TRUE',
        Or: 'FALSE'
    }

    def connect(self, *largs, **kargs):
        for arg in largs:
            d = None
            if isinstance(arg, basestring):
                kargs["dsn"] = arg
            elif isinstance(arg, ModuleType):
                d = arg.__dict__
            elif isinstance(arg, dict):
                d = arg
            else:
                raise TypeError("unknown argument: %s" % arg)
            if d is not None:
                for k in d:
                    if k[:1] != '_':
                        kargs[k] = d[k]
        self.db = psycopg2.connect(**kargs)

    def cursor(self, **kargs):
        try:
            lookup(kargs, 'cursor_factory')
        except KeyError:
            kargs['cursor_factory'] = psycopg2.extras.DictCursor
        return self.db.cursor(**kargs)

    def binaryOp(self, op, left, right):
        if op.__class__ == Like and op.case:
            return '%s ILIKE %s' % (left, right)
        return SQLDB.binaryOp(self, op, left, right)

    def makeType(self, t):
        if isinstance(t, tuple):
            t, c = t
        else:
            c = set()
        if 'autoincrement' in c:
            t = enumerate
            c = set(c)
            c.remove('autoincrement')
            c.add('primary_key')
        return SQLDB.makeType(self, (t, c))

    def createIndex(self, cur, name, idx):
        try:
            if isinstance(idx, tuple):
                cols, cons = idx
            else:
                cols = idx
                cons = set()
            cols = enlist(cols)
            idxname = self.quoteIdent('idx_%s_%s' % (name,
                                                '_'.join(cols + list(cons))))
            cur.execute('SELECT to_regclass(%s)', ['public.%s' % idxname])
            if cur.fetchone()[0] is None:
                idxcols = ', '.join(self.quoteIdent(col) for col in cols)
                unique = 'UNIQUE ' if 'unique' in cons else ''
                cur.execute('CREATE %sINDEX %s ON %s(%s)'
                        % (unique, idxname, self.quoteIdent(name), idxcols))
        except psycopg2.ProgrammingError as ex:
            self.db.rollback()
            raise ex

    def returning(self, id):
        if id is not None:
            return ' RETURNING %s' % self.quoteIdent(id)
        return ''

    def limit(self, limit = None, offset = None):
        out = ''
        if limit is not None:
            out += ' LIMIT %d' % limit
        if offset is not None:
            out += ' OFFSET %d' % offset
        return out

    def lastrowid(self, cur):
        return cur.fetchone()[0]

    def __str__(self):
        d = dict(x.split('=') for x in self.db.dsn.split())
        host = d["host"]
        if "user" in d:
            host = "%s@%s" % (d["user"], host)
        if "port" in d:
            host = "%s:%s" % (host, d["port"])
        return 'PostgreSQL database "%s" at %s' % (d["dbname"], host)

PostgreSQLDB.types = dict(SQLDB.types)
PostgreSQLDB.convert_to = dict(SQLDB.convert_to)
PostgreSQLDB.binaryops = dict(SQLDB.binaryops)
PostgreSQLDB.types[bool] = 'BOOLEAN'
PostgreSQLDB.types[enumerate] = 'SERIAL'
PostgreSQLDB.convert_to[bool] = bool
PostgreSQLDB.binaryops[Power] = '^'
PostgreSQLDB.binaryops[BitwiseXOr] = '#'
