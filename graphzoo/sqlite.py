import errno
import os
import shutil
import sqlite3
from sqldb import SQLDB

DBFILE = os.path.join(os.path.expanduser('~'), '.graphzoo', 'graphzoo.db')

class SQLiteDB(SQLDB):
    data_string = '?'
    ident_quote = '"'
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

    def createIndex(self, cur, name, col):
        cur.execute('CREATE INDEX IF NOT EXISTS %s ON %s(%s)'
                        % (self.quoteIdent('idx_%s_%s' % (name, col)),
                            self.quoteIdent(name), self.quoteIdent(col)))

    def importDB(self, file):
        # TODO: import data in the database instead of replacing it!
        file = os.path.expanduser(file)
        if not os.path.isfile(file):
            raise OSError(errno.ENOENT)
        self.db.close()
        shutil.copy(file, self.file)
        self.connect(file = self.file)
