import errno
import os
import sqlite3
from sqldb import SQLDB

DBFILE = os.path.join(os.path.expanduser("~"), ".graphzoo", "graphzoo.db")

class SQLiteDB(SQLDB):
    data_string = "?"
    
    def connect(self, file = DBFILE):
        try:
            os.makedirs(os.path.dirname(file))
        except OSError as ex:
            if ex.errno != errno.EEXIST:
                raise ex
        self.db = sqlite3.connect(file)
        self.db.text_factory = str
        self.db.row_factory = sqlite3.Row
        self.rowtype = sqlite3.Row
