from sage.graphs.graph import Graph
from sage.rings.integer import Integer
from ..query import Table
from ..utility import isinteger
from ..utility import lookup
from ..zooentity import ZooInfo
from ..zoograph import ZooGraph
from ..zoograph import canonical_label
from ..zoograph import override
from ..zooobject import ZooObject

class CVTGraph(ZooGraph):
    _cvtprops = None
    _parent = ZooGraph
    _spec = None
    _dict = "_cvtprops"

    def __init__(self, data = None, index = None, **kargs):
        ZooObject._init_(self, CVTGraph, kargs, defNone = ["order"],
                         setVal = {"data": data, "index": index},
                         setProp = {"cvt_index": "index"})

    def _parse_params(self, d):
        if isinteger(d["data"]):
            if d["index"] is None:
                d["zooid"] = Integer(d["data"])
            else:
                d["order"] = Integer(d["data"])
            d["data"] = None
            return True
        else:
            return False

    def _clear_params(self, d):
        d["order"] = None
        d["index"] = None

    def _construct_object(self, cl, d):
        if d["order"] is not None and d["index"] is not None:
            join = Table(cl._spec["name"]).join(Table(cl._parent._spec["name"]),
                         by = {cl._spec["primary_key"]})
            r = self._db_read(cl._parent, join, {"order": d["order"],
                                                 "cvt_index": d["index"]})
            d["zooid"] = r["zooid"]
            d["graph"] = None
        ZooGraph.__init__(self, **d)

        if d["order"] is not None:
            assert(d["order"] == self._graphprops["order"])
        if len(self._cvtprops) == 0:
            self._db_read(cl)

    def _repr_generic(self):
        return "cubic vertex-transitive graph on %d vertices, number %d" \
                                            % (self.order(), self.cvt_index())

    def cvt_index(self):
        return lookup(self._cvtprops, "cvt_index")

    @override.derived
    def is_moebius_ladder(self, store = False):
        g = self.girth(store = store)
        if g != 4:
            return False
        o = self.order(store = store)
        b = self.is_bipartite(store = store)
        if o == 6:
            return b
        d = self.diameter(store = store)
        og = self.odd_girth(store = store)
        return ((o % 4 == 0 and 4*d == o and og == 2*d+1) or
                    (o % 4 == 2 and 4*d == o+2 and b)) and \
                len(self.distance_graph(2)[next(self.vertex_iterator())]) == 4

    @override.derived
    def is_prism(self, store = False):
        o = self.order(store = store)
        b = self.is_bipartite(store = store)
        if o == 6:
            return not b
        if o == 8:
            return b
        g = self.girth(store = store)
        if g != 4:
            return False
        d = self.diameter(store = store)
        og = self.odd_girth(store = store)
        return ((o % 4 == 0 and 4*d == o+4 and b) or
                    (o % 4 == 2 and 4*d == o+2 and og == 2*d-1)) and \
                len(self.distance_graph(2)[next(self.vertex_iterator())]) == 4

info = ZooInfo(CVTGraph)

def import_cvt(file, db = None, format = "sparse6", verbose = False):
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
            n = g.order()
            if n > previous:
                if verbose and n > 0:
                    print "Imported %d graphs of order %d" % (i, previous)
                previous = n
                i = 0
            i += 1
            CVTGraph(graph = g, order = n, index = i, cur = cur, db = db)
        if verbose:
            print "Imported %d graphs of order %d" % (i, n)
        f.close()
    cur.close()
    db.commit()
