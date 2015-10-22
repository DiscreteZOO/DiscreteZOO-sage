from sage.graphs.graph import Graph
from utility import lookup
from utility import todict
import sqlite

class ZooGraph(Graph):
    _zooid = None
    _props = {}
    
    def __init__(self, zooid = None, data = None, props = None):
        if data != None:
            if props != None:
                self._props = props
        else:
            if zooid == None:
                raise IndexError("graph id not given")
            cur = sqlite.db.cursor()
            cur.execute("SELECT * FROM graph WHERE id = ?", [int(zooid)])
            r = cur.fetchone()
            cur.close()
            if r == None:
                raise KeyError(zooid)
            self._props = todict(r, skip = ["id", "data"])
            data = r["data"]
        self._zooid = zooid
        Graph.__init__(self, data)

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

    def diameter(self, by_weight=False, algorithm = None, weight_function=None,
                 check_weight=True, store = False):
        default = not by_weight and algorithm == None and \
                  weight_function == None and check_weight
        try:
            if not default:
                    raise NotImplementedError
            return lookup(self._props, "diameter")
        except (KeyError, NotImplementedError):
            d = Graph.diameter(self, by_weight, algorithm, weight_function,
                               check_weight)
            if default and store:
                self._props["diameter"] = d
            return d

    def is_regular(self, k = None):
        try:
            r = lookup(self._props, "is_regular")
            if k == None:
                return r >= 0
            else:
                return r == k
        except KeyError:
            r = Graph.is_regular(self, k)
            if store and (r ^ (k == None)):
                if not r:
                    self._props["is_regular"] = -1
                else:
                    self._props["is_regular"] = k
            return r
