r"""
SQLite database interface

This module defines an interface class implementing the peculiarities of the
SQLite database.
"""

import errno
import os
import shutil
import sqlite3
from .query import Modulo
from .query import enlist
from .sqldb import SQLDB

# The default location for the database
DBFILE = os.path.join(os.path.expanduser('~'),
                      '.discretezoo', 'discretezoo.db')


class SQLiteDB(SQLDB):
    r"""
    An interface class for the SQLite database.
    """

    data_string = '?'
    ident_quote = '"'
    exceptions = sqlite3.Error
    file = None

    def connect(self, file=DBFILE):
        r"""
        Connect to the database.

        INPUT:

        - ``file`` - the file containing the database (default: ``DBFILE``).
        """
        dir = os.path.dirname(file)
        if dir:
            try:
                os.makedirs(dir)
            except OSError as ex:
                if ex.errno != errno.EEXIST:
                    raise ex
        self.file = file
        self.db = sqlite3.connect(file)
        self.db.text_factory = str
        self.db.row_factory = sqlite3.Row

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
        if isinstance(idx, tuple):
            cols, cons = idx
        else:
            cols = idx
            cons = set()
        cols = enlist(cols)
        idxname = self.quoteIdent('idx_%s_%s' % (name,
                                                 '_'.join(cols + list(cons))))
        idxcols = ', '.join(self.quoteIdent(col) for col in cols)
        unique = 'UNIQUE ' if 'unique' in cons else ''
        cur.execute('CREATE %sINDEX IF NOT EXISTS %s ON %s(%s)' %
                    (unique, idxname, self.quoteIdent(name), idxcols))

    def importDB(self, file):
        r"""
        Import the database from a file.

        Currently, simply replaces the current database file with a copy of the
        specified file. A non-destructive import of new objects and properties
        is planned for the future.

        INPUT:

        - ``file`` - the file containing the new database.
        """
        # TODO: import data in the database instead of replacing it!
        file = os.path.expanduser(file)
        if not os.path.isfile(file):
            raise OSError(errno.ENOENT)
        self.db.close()
        shutil.copy(file, self.file)
        self.connect(file=self.file)

    def __str__(self):
        return 'SQLite database in %s' % self.file


# SQLite-specific keywords and symbols
SQLiteDB.constraints = dict(SQLDB.constraints)
SQLiteDB.constraints['autoincrement'] = 'PRIMARY KEY AUTOINCREMENT'
SQLiteDB.binaryops = dict(SQLDB.binaryops)
SQLiteDB.binaryops[Modulo] = '%'
