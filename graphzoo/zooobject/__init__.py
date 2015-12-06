__all__ = ['fields', 'ZooObject', 'info']

from sage.rings.integer import Integer
from .zooobject import *
from ..query import makeFields
import fields

objspec = {
    "name": "object",
    "primary_key": "id",
    "indices": {},
    "skip": {"id", "unique_id"},
    "fields" : {
        "id": (Integer, {"autoincrement"}),
        "unique_id": (str, {"unique"})
    },
    "compute": {},
    "default": {}
}

ZooObject._spec = objspec
makeFields(ZooObject, fields)
