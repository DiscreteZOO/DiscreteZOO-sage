__all__ = ['Change']

from sage.rings.integer import Integer
from .change import *
from ..zooentity import ZooEntity

objspec = {
    "name": "change",
    "primary_key": "change_id",
    "indices": [(["zooid", "table", "column", "commit"], {"unique"})],
    "skip": {"change_id", "zooid"},
    "fields" : {
        "change_id": (Integer, {"autoincrement"}),
        "column": (str, {"not_null"}),
        "commit": (str, {"not_null"}),
        "table": (str, {"not_null"}),
        "user": str,
        "zooid": (ZooEntity, {"not_null"})
    },
    "compute": {},
    "default": {}
}

Change._spec = objspec
