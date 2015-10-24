from sage.rings.integer import Integer
from sage.graphs.graph import GenericGraph
from sage.graphs.graph import Graph
from utility import lookup
from utility import update
from utility import todict
from utility import isinteger
import sqlite

class ZooGraph(Graph):
    _zooid = None
    _props = {}
    
    def __init__(self, data = None, zooid = None, props = None, graph = None,
                 name = None, **kargs):
        if isinteger(data):
            zooid = Integer(data)
            data = None
        elif isinstance(data, GenericGraph):
            graph = data
            data = None
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
        Graph.__init__(self, data = data, name = name, **kargs)

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

    def order(self, store = False, **kargs):
        default = len(kargs) == 0
        try:
            if not default:
                raise NotImplementedError
            return lookup(self._props, "vertices")
        except (KeyError, NotImplementedError):
            o = Graph.order(self, **kargs)
            if default and store:
                update(self._props, "vertices", o)
            return o

    def girth(self, store = False, **kargs):
        default = len(kargs) == 0
        try:
            if not default:
                raise NotImplementedError
            return lookup(self._props, "girth")
        except (KeyError, NotImplementedError):
            g = Graph.girth(self, **kargs)
            if default and store:
                update(self._props, "girth", g)
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
                update(self._props, "diameter", d)
            return d

    def is_regular(self, k = None, store = False, **kargs):
        default = len(kargs) == 0
        try:
            if not default:
                raise NotImplementedError
            r = lookup(self._props, "is_regular")
            if k is None:
                return r >= 0
            else:
                return r == k
        except (KeyError, NotImplementedError):
            r = Graph.is_regular(self, k, **kargs)
            if default and store and (r ^ (k is None)):
                update(self._props, "is_regular", k if r else -1)
            return r
