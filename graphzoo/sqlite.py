import sqlite3
import os
import errno

DBFILE = os.path.join(os.path.expanduser("~"), ".graphzoo", "graphzoo.db")

def connect(file):
    try:
        os.makedirs(os.path.dirname(file))
    except OSError as ex:
        if ex.errno != errno.EEXIST:
            raise ex
    db = sqlite3.connect(file)
    db.text_factory = str
    db.row_factory = sqlite3.Row
    return db

db = connect(DBFILE)

def initdb():
    db.execute("""
        CREATE TABLE IF NOT EXISTS graph (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            data        TEXT NOT NULL,
            vertices    INTEGER,
            girth       INTEGER,
            diameter    INTEGER,
            is_regular  INTEGER
        )
    """)
    db.commit()
