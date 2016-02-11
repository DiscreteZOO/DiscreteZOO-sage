__all__ = ["entities", "db"]
DEFAULT_DB = None
WRITE_TO_DB = True
TRACK_CHANGES = True

from util.install import install
install()

from entities import *
from db import *

DEFAULT_DB = sqlite.SQLiteDB(track = TRACK_CHANGES)
info = zoograph.info

A = query.A
C = query.Column
V = query.Value
Asc = query.Ascending
Desc = query.Descending
