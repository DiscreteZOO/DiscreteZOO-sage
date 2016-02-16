from sage.graphs.digraph import DiGraph
from sage.graphs.graph import Graph
from sage.rings.integer import Integer
import discretezoo
from ..vt import VTGraph
from ..zooentity import ZooInfo
from ..zoograph import ZooGraph
from ..zoograph import import_graphs
from ..zoograph import override
from ..zooobject import ZooObject
from ...db.query import Table
from ...util.utility import isinteger
from ...util.utility import lookup
from ...util.utility import update

class CVTGraph(VTGraph):
    _cvtprops = None
    _parent = VTGraph
    _spec = None
    _dict = "_cvtprops"

    def __init__(self, data = None, index = None, symcubic_index = None,
                 **kargs):
        ZooObject._init_(self, CVTGraph, kargs,
                         defNone = ["order"],
                         setVal = {"data": data, "cvt_index": index,
                                   "symcubic_index": symcubic_index},
                         setProp = {"cvt_index": "cvt_index",
                                    "symcubic_index": "symcubic_index"})

    def _parse_params(self, d):
        if isinteger(d["data"]):
            if d["cvt_index"] is None and d["symcubic_index"] is None:
                d["zooid"] = Integer(d["data"])
            else:
                d["order"] = Integer(d["data"])
            d["data"] = None
            return True
        else:
            return VTGraph._parse_params(self, d)

    def _construct_object(self, cl, d):
        if d["order"] is not None:
            cond = {"order": d["order"]}
            if d["cvt_index"] is not None:
                cond["cvt_index"] = d["cvt_index"]
            elif d["symcubic_index"] is not None:
                cond["symcubic_index"] = d["symcubic_index"]
            if len(cond) > 1:
                join = Table(cl._spec["name"]).join(
                                    Table(ZooGraph._spec["name"]),
                                    by = frozenset([cl._spec["primary_key"]]))
                try:
                    r = self._db_read(ZooGraph, join, cond)
                    d["zooid"] = r["zooid"]
                    d["graph"] = None
                except KeyError:
                    pass
        VTGraph.__init__(self, **d)

        if d["order"] is not None:
            assert(d["order"] == self._graphprops["order"])
        if len(self._cvtprops) == 0:
            self._db_read(cl)

    def _repr_generic(self):
        index = self.cvt_index()
        tr = "vertex-transitive"
        if index is None:
            index = self.symcubic_index()
            if index is not None or \
                    lookup(self._graphprops, "is_arc_transitive",
                           default = False):
                tr = "symmetric"
        out = "cubic %s graph on %d vertices" % (tr, self.order())
        if index is not None:
            out = "%s, number %s" % (out, index)
        return out

    def cvt_index(self):
        return lookup(self._cvtprops, "cvt_index", default = None)

    @override.computed
    def is_moebius_ladder(self, store = discretezoo.WRITE_TO_DB, cur = None):
        g = self.girth(store = store, cur = cur)
        if g != 4:
            return False
        o = self.order(store = store, cur = cur)
        b = self.is_bipartite(store = store, cur = cur)
        if o == 6:
            return b
        d = self.diameter(store = store, cur = cur)
        og = self.odd_girth(store = store, cur = cur)
        return ((o % 4 == 0 and 4*d == o and og == 2*d+1) or
                    (o % 4 == 2 and 4*d == o+2 and b)) and \
                len(self.distance_graph(2)[next(self.vertex_iterator())]) == 4

    @override.computed
    def is_prism(self, store = discretezoo.WRITE_TO_DB, cur = None):
        o = self.order(store = store, cur = cur)
        b = self.is_bipartite(store = store, cur = cur)
        if o == 6:
            return not b
        if o == 8:
            return b
        g = self.girth(store = store, cur = cur)
        if g != 4:
            return False
        d = self.diameter(store = store, cur = cur)
        og = self.odd_girth(store = store, cur = cur)
        return ((o % 4 == 0 and 4*d == o+4 and b) or
                    (o % 4 == 2 and 4*d == o+2 and og == 2*d-1)) and \
                len(self.distance_graph(2)[next(self.vertex_iterator())]) == 4

    def symcubic_index(self):
        return lookup(self._cvtprops, "symcubic_index", default = None)

    def truncation(self, name = None, store = discretezoo.WRITE_TO_DB,
                   cur = None):
        commit = False
        if lookup(self._graphprops, "is_arc_transitive", default = False):
            cl = CVTGraph
        else:
            cl = ZooGraph
        try:
            t = lookup(self._cvtprops, "truncation")
            if isinteger(t):
                t = cl(zooid = t, db = self._db)
                update(self._cvtprops, "truncation", t)
        except KeyError:
            G = Graph([DiGraph(self).edges(labels = False),
                       lambda (u, v), (x, y): u == x or (u, v) == (y, x)],
                      loops = False)
            if not store:
                cur = None
            elif cur is None:
                cur = self._db.cursor()
                commit = True
            try:
                t = cl(G, db = self._db, cur = cur)
            except KeyError:
                t = G
            if store:
                self._update_rows(CVTGraph, {"truncation": t._zooid},
                                  {self._spec["primary_key"]: self._zooid},
                                  cur = cur)
        if name is None:
            nm = self.name()
            if nm and not t.name():
                name = "Truncated %s" % nm
        if name is not None:
            if isinstance(t, ZooGraph):
                t.name(new = name, store = store, cur = cur)
            else:
                t.name(new = name)
        if commit:
            self._db.commit()
        update(self._cvtprops, "truncation", t)
        return t

def import_cvt(file, db = None, format = "sparse6", index = "index",
               verbose = False):
    import_graphs(file, cl = CVTGraph, db = db, format = format, index = index,
                  verbose = verbose)

info = ZooInfo(CVTGraph)
