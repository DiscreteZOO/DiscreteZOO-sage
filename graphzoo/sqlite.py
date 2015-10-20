import sqlite3
import os

DBFILE = os.path.join(os.path.expanduser("~"), ".graphzoo", "graphzoo.db")

class Database:
    db = None
    
    def __init__(self, file):
        os.makedirs(os.path.dirname(file))
        self.db = sqlite3.connect(file)

db = Database(DBFILE)
