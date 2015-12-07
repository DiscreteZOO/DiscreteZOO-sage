__all__ = ['fields', 'ZooObject', 'info']

from sage.rings.integer import Integer
from .zooobject import *
from ..query import makeFields
import fields

objspec = {
    "name": "object",
    "primary_key": "zooid",
    "indices": {},
    "skip": {"unique_id", "zooid"},
    "fields" : {
        "unique_id": (str, {"unique"}),
        "zooid": (Integer, {"autoincrement"})
    },
    "compute": {},
    "default": {}
}

ZooObject._spec = objspec
makeFields(ZooObject, fields)
