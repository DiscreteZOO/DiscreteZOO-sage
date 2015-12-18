__all__ = ['ZooEntity', 'ZooInfo']

from sage.rings.integer import Integer
from .zooentity import *

objspec = {
    "name": "entity",
    "primary_key": "zooid",
    "indices": {},
    "skip": {"zooid"},
    "fields" : {
        "zooid": (Integer, {"autoincrement"})
    },
    "compute": {},
    "default": {}
}

ZooEntity._spec = objspec
