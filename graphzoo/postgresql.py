import psycopg2, psycopg2.extensions, psycopg2.extras
from query import And
from query import BitwiseXOr
from query import Like
from query import Or
from query import Power
from sqldb import SQLDB
from utility import lookup

class PostgreSQLDB(SQLDB):
    data_string = '%s'
    ident_quote = '"'

    logicalconsts = {
        And: 'TRUE',
        Or: 'FALSE'
    }

    def connect(self, **kargs):
        self.db = psycopg2.connect(**kargs)

        self.types[bool] = 'BOOLEAN'
        self.convert_to[bool] = bool
        self.binaryops[Power] = '^'
        self.binaryops[BitwiseXOr] = '#'
        self.types[enumerate] = 'SERIAL'

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
        if type(t) == tuple:
            t, c = t
        else:
            c = set()
        if 'autoincrement' in c:
            t = enumerate
            c.remove('autoincrement')
            c.add('primary_key')
        return SQLDB.makeType(self, (t, c))

    def createIndex(self, cur, name, col):
        idxname = 'idx_%s_%s' % (name, col)
        cur.execute('SELECT to_regclass(%s)', ['public.%s' % idxname])
        if cur.fetchone()[0] is None:
            cur.execute('CREATE INDEX %s ON %s(%s)' %
                            (self.quoteIdent(idxname), self.quoteIdent(name),
                                self.quoteIdent(col)))

    def returning(self, id):
        if id is not None:
            return ' RETURNING %s' % self.quoteIdent(id)
        return ''

    def lastrowid(self, cur):
        return cur.fetchone()[0]
