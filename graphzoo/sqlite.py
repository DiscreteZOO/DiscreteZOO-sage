import errno
import os
import shutil
import sqlite3
from sqldb import SQLDB

DBFILE = os.path.join(os.path.expanduser('~'), '.graphzoo', 'graphzoo.db')

class SQLiteDB(SQLDB):
    data_string = '?'
    ident_quote = '"'
    exceptions = sqlite3.Error
    file = None
    
    def connect(self, file = DBFILE):
        try:
            os.makedirs(os.path.dirname(file))
        except OSError as ex:
            if ex.errno != errno.EEXIST:
                raise ex
        self.file = file
        self.db = sqlite3.connect(file)
        self.db.text_factory = str
        self.db.row_factory = sqlite3.Row

        self.constraints['autoincrement'] = 'PRIMARY KEY AUTOINCREMENT'

    def createIndex(self, cur, name, idx):
        if isinstance(idx, tuple):
            cols, cons = idx
        else:
            cols = idx
            cons = set()
        if isinstance(cols, set):
            cols = sorted(cols)
        elif not isinstance(cols, list):
            cols = [cols]
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
