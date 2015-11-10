__all__ = ["zoograph", "cvt", "sqlite", "query"]
DEFAULT_DB = None
from . import *

DEFAULT_DB = sqlite.SQLiteDB()
info = zoograph.info

C = query.Column
V = query.Value
