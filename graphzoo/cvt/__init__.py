__all__ = ['fields', 'CVTGraph', 'info']

from sage.graphs.graph import GenericGraph
from sage.graphs.graph import Graph
from sage.rings.integer import Integer
from ..query import Table
from ..utility import isinteger
from ..utility import lookup
from ..zoograph import ZooGraph
from ..zooobject import ZooInfo
from ..zooobject import ZooObject

_objspec = {
    "name": "graph_cvt",
    "dict": "_cvtprops",
    "primary_key": "id",
    "indices": {"cvt_index"},
    "skip": {"id"},
    "fields" : {
        "cvt_index": Integer,
        "id": (ZooGraph, {"primary_key"})
    },
    "compute": {},
    "default": {
        "_props": {
            "average_degree": 3,
            "is_regular": True,
            "is_tree": False,
            "is_vertex_transitive": True
        }
    }
}

class CVTGraph(ZooGraph):
    _cvtprops = None
    _spec = _objspec
    _parent = ZooGraph

    def __init__(self, data = None, index = None, vertices = None,
                 zooid = None, props = None, graph = None, name = None,
                 cur = None, db = None, **kargs):
        ZooObject.__init__(self, db)
        cl = CVTGraph
        if isinteger(data):
            if index is None:
                zooid = Integer(data)
            else:
                vertices = Integer(data)
            data = None
        else:
            data, props, graph, zooid = self._init_params(data, props, graph,
                                                          zooid)

        if graph is not None:
            data, name, zooid = self._init_graph(cl, graph, name, cur, zooid)
            vertices = None
            index = None
        else:
            cur = None
            props = self._init_props(cl, props)

        if vertices is not None and index is not None:
            join = Table(self._spec["name"]).join(Table(self._parent._spec["name"]),
                         by = {self._spec["primary_key"]})
            r = self._db_read(self._parent, join,
                                {"order": vertices, "cvt_index": index})
            ZooGraph.__init__(self, zooid = r["id"], data = r["data"],
                              props = props, name = name, db = db, **kargs)
        else:
            ZooGraph.__init__(self, zooid = zooid, data = data, graph = graph,
                              props = props, name = name, cur = cur, db = db,
                              **kargs)
        if vertices is not None:
            assert(vertices == self._props["order"])
        if self._cvtprops is None:
            self._db_read(cl)
        if cur is not None:
            self._db_write_cvt(cl, cur)

    def _repr_(self):
        name = "Cubic vertex-transitive graph on %d vertices, number %d" \
                                            % (self.order(), self.cvt_index())
        if self.name() != '':
            name = self.name() + ": " + name
        return name

    def cvt_index(self):
        return lookup(self._cvtprops, "cvt_index")

info = ZooInfo(CVTGraph)

def import_cvt(file, db = None, format = "sparse6", canonical = False,
               verbose = False):
    if db is None:
        db = info.getdb()
    info.initdb(db = db, commit = False)
    previous = 0
    i = 0
    cur = db.cursor()
    with open(file) as f:
        for line in f:
            data = line.strip()
            if format not in ["graph6", "sparse6"]:
                data = eval(data)
            g = Graph(data)
            if canonical:
                g = g.canonical_label(algorithm = "sage")
            n = g.order()
            if n > previous:
                if verbose and n > 0:
                    print "Imported %d graphs of order %d" % (i, previous)
                previous = n
                i = 0
            i += 1
            CVTGraph(graph = g, vertices = n, index = i, cur = cur, db = db)
        if verbose:
            print "Imported %d graphs of order %d" % (i, n)
        f.close()
    cur.close()
    db.commit()
