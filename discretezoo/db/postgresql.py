r"""
PostgreSQL database interface

This module defines an interface class implementing the peculiarities of the
PostgreSQL database.
"""

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
    r"""
    An interface class for the PostgreSQL database.
    """

    data_string = '%s'
    ident_quote = '"'
    exceptions = psycopg2.Error

    logicalconsts = {
        And: 'TRUE',
        Or: 'FALSE'
    }

    def connect(self, *largs, **kargs):
        r"""
        Connect to the database.

        INPUT:

        - an unnamed string parameter will be passed to the Python interface's
          ``connect`` method under the keyword ``dsn``. It should be a string
          specifying the connection parameters.

        - a module parameter will have each of its fields passed to the Python
          interface's ``connect`` method with its name.

        - a dictionary parameter will have each of its values passed to the
          Python interface's ``connect`` method with the name of the
          corresponding key.

        - any named parameter will be passed to the Python interface's
          ``connect`` method under the same name.

        The named parameters obtained as above may be:

        - ``database`` - the database name.

        - ``user`` - the user name.

        - ``password`` - the password used to authenticate

        - ``host`` - the address of the database server.

        - ``port`` - port number.

        Please refer to the psycopg2 manual for details.
        """
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
        r"""
        Return a cursor.

        INPUT:

        - ``cursor_factory`` - the cursor factory to be used. The default
          value of ``psycopg2.extras.DictCursor`` provides rows with
          dictionary-like access.

        Any keyword input is forwarded to the Python database interface's
        ``cursor`` method.
        """
        try:
            lookup(kargs, 'cursor_factory')
        except KeyError:
            kargs['cursor_factory'] = psycopg2.extras.DictCursor
        return self.db.cursor(**kargs)

    def binaryOp(self, op, left, right):
        r"""
        Format a SQL binary operation.

        PostgreSQL supports an ILIKE operator for case-insensitive string
        comparison, which this method provides support for. See the
        ``SQLDB.binaryOp`` method for notes on division.

        INPUT:

        - ``op`` - the binary operator.

        - ``left`` - an SQL string representing the left operand.

        - ``right`` - an SQL string representing the right operand.
        """
        if op.__class__ == Like and op.case:
            return '%s ILIKE %s' % (left, right)
        return SQLDB.binaryOp(self, op, left, right)

    def makeType(self, t, c):
        r"""
        Format a type and constraint specification.

        Since PostgreSQL implements auto-incrementing by means of a SERIAL
        pseudo-type, this is handled in this method.

        INPUT:

        - ``t`` - the Sage/Python type of the represented object. Subclasses of
          ``ZooEntity`` are represented by a foreign key to the appropriate
          table.

        - ``c`` - a collection of column constraints.
        """
        if 'autoincrement' in c:
            t = enumerate
            c = set(c)
            c.remove('autoincrement')
            c.add('primary_key')
        return SQLDB.makeType(self, t, c)

    def createIndex(self, cur, name, idx):
        r"""
        Create an index.

        Currently, only unique constraints are respected.

        INPUT:

        - ``cur`` - the cursor to be used.

        - ``name`` - the table on which the index is to be created.

        - ``idx`` - a list of columns to be indexed. May also be a tuple
          containing said list and a collection of constraints.
        """
        try:
            if isinstance(idx, tuple):
                cols, cons = idx
            else:
                cols = idx
                cons = set()
            cols = enlist(cols)
            idxname = self.quoteIdent('idx_%s_%s' %
                                      (name, '_'.join(cols + list(cons))))
            cur.execute('SELECT to_regclass(%s)', ['public.%s' % idxname])
            if cur.fetchone()[0] is None:
                idxcols = ', '.join(self.quoteIdent(col) for col in cols)
                unique = 'UNIQUE ' if 'unique' in cons else ''
                cur.execute('CREATE %sINDEX %s ON %s(%s)' %
                            (unique, idxname, self.quoteIdent(name), idxcols))
        except psycopg2.ProgrammingError as ex:
            self.db.rollback()
            raise ex

    def returning(self, id):
        r"""
        Format a RETURNING expression.

        INPUT:

        - ``id`` - the name of the ID column.
        """
        if id is not None:
            return ' RETURNING %s' % self.quoteIdent(id)
        return ''

    def limit(self, limit=None, offset=None):
        r"""
        Format a LIMIT clause.

        PostgreSQL allows using OFFSET even when LIMIT is not specified.

        INPUT:

        - ``limit`` - the maximal number of rows to output (default: ``None``).

        - ``offset`` - the number of rows to skip (default: ``None``).
        """
        out = ''
        if limit is not None:
            out += ' LIMIT %d' % limit
        if offset is not None:
            out += ' OFFSET %d' % offset
        return out

    def lastrowid(self, cur):
        r"""
        Return the ID of the last inserted row.

        Fetches the ID by fetching a row given by the RETURNING clause.

        INPUT:

        - ``cur`` - the cursor to be used.
        """
        return cur.fetchone()[0]

    def __str__(self):
        d = dict(x.split('=') for x in self.db.dsn.split())
        host = d["host"]
        if "user" in d:
            host = "%s@%s" % (d["user"], host)
        if "port" in d:
            host = "%s:%s" % (host, d["port"])
        return 'PostgreSQL database "%s" at %s' % (d["dbname"], host)


# PostgreSQL-specific keywords and symbols
PostgreSQLDB.types = dict(SQLDB.types)
PostgreSQLDB.convert_to = dict(SQLDB.convert_to)
PostgreSQLDB.binaryops = dict(SQLDB.binaryops)
PostgreSQLDB.types[bool] = 'BOOLEAN'
PostgreSQLDB.types[enumerate] = 'SERIAL'
PostgreSQLDB.convert_to[bool] = bool
PostgreSQLDB.binaryops[Power] = '^'
PostgreSQLDB.binaryops[BitwiseXOr] = '#'
