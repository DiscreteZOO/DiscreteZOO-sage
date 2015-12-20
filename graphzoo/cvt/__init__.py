__all__ = ['fields', 'CVTGraph', 'info']

from sage.rings.integer import Integer
from .cvtgraph import *
from ..query import makeFields
from ..zoograph import ZooGraph
import fields

objspec = {
    "name": "graph_cvt",
    "primary_key": "zooid",
    "indices": {"cvt_index"},
    "skip": {"zooid"},
    "fields" : {
        "cvt_index": Integer,
        "is_moebius_ladder": bool,
        "is_prism": bool,
        "is_spx": bool,
        "zooid": ZooGraph
    },
    "compute": {},
    "default": {
        ZooGraph: {
            "average_degree": 3,
            "is_regular": True,
            "is_tree": False,
            "is_vertex_transitive": True
        }
    }
}

CVTGraph._spec = objspec
makeFields(CVTGraph, fields)
