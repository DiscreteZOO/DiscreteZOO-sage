from sage.graphs.digraph import DiGraph
from sage.graphs.graph import Graph
from sage.rings.integer import Integer
import discretezoo
from ..zooentity import ZooInfo
from ..zoograph import ZooGraph
from ..zoograph import override
from ..zooobject import ZooObject
from ...db.query import Table
from ...util.utility import isinteger
from ...util.utility import lookup
from ...util.utility import update

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
            return ZooGraph._parse_params(self, d)

    def _clear_params(self, d):
        d["order"] = None
        d["index"] = None

    def _construct_object(self, cl, d):
        if d["order"] is not None and d["index"] is not None:
            join = Table(cl._spec["name"]).join(Table(cl._parent._spec["name"]),
                         by = frozenset([cl._spec["primary_key"]]))
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

    def truncation(self, store = discretezoo.WRITE_TO_DB, cur = None):
        if lookup(self._graphprops, "is_arc_transitive", default = False):
            cl = CVTGraph
        else:
            cl = ZooGraph
        try:
            t = lookup(self._cvtprops, "truncation")
            if isinteger(t):
                t = cl(zooid = t, db = self._db)
                update(self._cvtprops, "truncation", t)
            return t
        except KeyError:
            pass
        commit = False
        G = Graph([DiGraph(self).edges(labels = False),
                   lambda (u, v), (x, y): u == x or (u, v) == (y, x)],
                  loops = False)
        try:
            t = cl(G, db = self._db)
        except KeyError as ex:
            if not store:
                raise ex
            if cur is None:
                cur = self._db.cursor()
                commit = True
            t = cl(G, db = self._db, cur = cur)
        if store:
            self._update_rows(CVTGraph, {"truncation": t._zooid},
                        {self._spec["primary_key"]: self._zooid},
                        cur = cur)
        if commit:
            self._db.commit()
        update(self._cvtprops, "truncation", t)
        return t

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
