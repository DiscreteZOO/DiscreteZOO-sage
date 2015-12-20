__all__ = ['fields', 'ZooObject', 'info']

from .zooobject import *
from ..query import makeFields
from ..utility import init_metaclasses
from ..zooentity import ZooEntity
from ..zooset import ZooSet
import fields

objspec = {
    "name": "object",
    "primary_key": "zooid",
    "indices": {},
    "skip": {"unique_id", "zooid"},
    "fields" : {
        "alias": ((ZooSet, {"alias": (str, {"not_null"})}), set()),
        "unique_id": (str, {"unique"}),
        "zooid": ZooEntity
    },
    "compute": {},
    "default": {}
}

ZooObject._spec = objspec
init_metaclasses(ZooObject)
makeFields(ZooObject, fields)
