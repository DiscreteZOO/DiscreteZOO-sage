__all__ = ['fields', 'SPXGraph', 'info']

from sage.rings.integer import Integer
from .spxgraph import *
from ..query import makeFields
from ..zoograph import ZooGraph
import fields

objspec = {
    "name": "graph_spx",
    "primary_key": "zooid",
    "indices": {"spx_r", "spx_s"},
    "skip": {"zooid"},
    "fields" : {
        "spx_r": Integer,
        "spx_s": Integer,
        "zooid": (ZooGraph, {"primary_key"})
    },
    "compute": {},
    "default": {
        ZooGraph: {
            "average_degree": 3,
            "is_bipartite": True,
            "is_regular": True,
            "is_tree": False
        }
    }
}

SPXGraph._spec = objspec
makeFields(SPXGraph, fields)
