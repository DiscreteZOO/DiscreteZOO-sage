__all__ = ['fields', 'SPXGraph', 'info']

from sage.rings.integer import Integer
from .spxgraph import *
from ..zoograph import ZooGraph
from ..cvt import CVTGraph

objspec = {
    "name": "graph_spx",
    "dict": "_spxprops",
    "primary_key": "id",
    "indices": {"spx_r", "spx_s"},
    "skip": {"id"},
    "fields" : {
        "id": (CVTGraph, {"primary_key"}),
        "spx_r": Integer,
        "spx_s": Integer
    },
    "compute": {},
    "default": {ZooGraph: {"is_bipartite": True}}
}

SPXGraph._spec = objspec
import fields
