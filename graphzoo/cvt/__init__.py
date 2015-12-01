__all__ = ['fields', 'CVTGraph', 'info']

from sage.rings.integer import Integer
from .cvtgraph import *
from ..query import makeFields
from ..zoograph import ZooGraph
import fields

objspec = {
    "name": "graph_cvt",
    "dict": "_cvtprops",
    "primary_key": "id",
    "indices": {"cvt_index"},
    "skip": {"id"},
    "fields" : {
        "cvt_index": Integer,
        "id": (ZooGraph, {"primary_key"}),
        "is_moebius_ladder": bool,
        "is_prism": bool,
        "is_spx": bool
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
makeFields(objspec, fields)
