__all__ = ["zooentity", "zoograph", "cvt", "spx", "sqlite", "query"]
DEFAULT_DB = None
WRITE_TO_DB = True
from . import *
from zooentity import initdb

DEFAULT_DB = sqlite.SQLiteDB()
info = zoograph.info

A = query.A
C = query.Column
V = query.Value
Asc = query.Ascending
Desc = query.Descending
