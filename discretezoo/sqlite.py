import errno
import os
import shutil
import sqlite3
from sqldb import SQLDB
from utility import enlist

DBFILE = os.path.join(os.path.expanduser('~'), '.discretezoo', 'discretezoo.db')

class SQLiteDB(SQLDB):
    data_string = '?'
    ident_quote = '"'
    exceptions = sqlite3.Error
    file = None
    
    def connect(self, file = DBFILE):
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
        cur.execute('CREATE %sINDEX IF NOT EXISTS %s ON %s(%s)'
                        % (unique, idxname, self.quoteIdent(name), idxcols))

    def importDB(self, file):
        # TODO: import data in the database instead of replacing it!
        file = os.path.expanduser(file)
        if not os.path.isfile(file):
            raise OSError(errno.ENOENT)
        self.db.close()
        shutil.copy(file, self.file)
        self.connect(file = self.file)

    def __str__(self):
        return 'SQLite database in %s' % self.file

SQLiteDB.constraints = dict(SQLDB.constraints)
SQLiteDB.constraints['autoincrement'] = 'PRIMARY KEY AUTOINCREMENT'
