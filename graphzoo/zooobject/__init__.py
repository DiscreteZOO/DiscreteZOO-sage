__all__ = ['fields', 'ZooObject', 'info']

from sage.rings.integer import Integer
from .zooobject import *
from ..query import makeFields
from ..utility import init_metaclasses
from ..zooset import ZooSet
import fields

objspec = {
    "name": "object",
    "primary_key": "zooid",
    "indices": {},
    "skip": {"unique_id", "zooid"},
    "fields" : {
        "alias": ((ZooSet, {"alias": (str, "not_null")}), set()),
        "unique_id": (str, {"unique"}),
        "zooid": (Integer, {"autoincrement"})
    },
    "compute": {},
    "default": {}
}

ZooObject._spec = objspec
init_metaclasses(ZooObject)
makeFields(ZooObject, fields)
