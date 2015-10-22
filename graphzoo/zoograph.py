from sage.graphs.graph import GenericGraph
from sage.graphs.graph import Graph
from utility import lookup
from utility import todict
import sqlite

class ZooGraph(Graph):
    _zooid = None
    _props = {}
    
    def __init__(self, zooid = None, data = None, props = None, graph = None,
                 name = None):
        if graph is not None:
            if not isinstance(graph, GenericGraph):
                raise TypeError("not a graph")
            data = graph
            if name is None:
                name = graph.name()
            if isinstance(graph, ZooGraph):
                zooid = graph._zooid
                props = graph._props
            elif zooid is None:
                raise IndexError("graph id not given")

        self._zooid = zooid
        if data is not None:
            if props is not None:
                self._props = props
        else:
            data = self._db_read()
        Graph.__init__(self, data = data, name = name)

    def _db_read(self):
        if self._zooid is None:
            raise IndexError("graph id not given")
        cur = sqlite.db.cursor()
        cur.execute("SELECT * FROM graph WHERE id = ?", [int(self._zooid)])
        r = cur.fetchone()
        cur.close()
        if r is None:
            raise KeyError(self._zooid)
        self._props = todict(r, skip = ["id", "data"])
        return r["data"]

    def load_db_data(self):
        self._db_read()

    def order(self, store = False):
        try:
            return lookup(self._props, "vertices")
        except KeyError:
            o = Graph.order(self)
            if store:
                self._props["vertices"] = o
            return o

    def girth(self, store = False):
        try:
            return lookup(self._props, "girth")
        except KeyError:
            g = Graph.girth(self)
            if store:
                self._props["girth"] = g
            return g

    def diameter(self, store = False, **kargs):
        default = len(kargs) == 0
        try:
            if not default:
                raise NotImplementedError
            return lookup(self._props, "diameter")
        except (KeyError, NotImplementedError):
            d = Graph.diameter(self, **kargs)
            if default and store:
                self._props["diameter"] = d
            return d

    def is_regular(self, k = None, store = False):
        try:
            r = lookup(self._props, "is_regular")
            if k is None:
                return r >= 0
            else:
                return r == k
        except KeyError:
            r = Graph.is_regular(self, k)
            if store and (r ^ (k is None)):
                if not r:
                    self._props["is_regular"] = -1
                else:
                    self._props["is_regular"] = k
            return r
