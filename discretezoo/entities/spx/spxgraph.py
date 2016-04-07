from sage.categories.cartesian_product import cartesian_product
from sage.rings.finite_rings.integer_mod_ring import Integers
from sage.rings.integer import Integer
from ..zooentity import ZooInfo
from ..zoograph import ZooGraph
from ..zooobject import ZooObject
from ...util.utility import isinteger
from ...util.utility import lookup

class SPXGraph(ZooGraph):
    _spxprops = None
    _parent = ZooGraph
    _spec = None
    _dict = "_spxprops"

    def __init__(self, data = None, s = None, **kargs):
        ZooObject._init_(self, SPXGraph, kargs, defNone = ["r"],
                         setVal = {"data": data, "s": s},
                         setProp = {"spx_r": "r", "spx_s": "s"})

    def _parse_params(self, d):
        if isinteger(d["data"]):
            if d["s"] is None:
                d["zooid"] = Integer(d["data"])
            else:
                d["r"] = Integer(d["data"])
            d["data"] = None
            return True
        else:
            return ZooGraph._parse_params(self, d)

    def _clear_params(self, d):
        d["r"] = None
        d["s"] = None

    def _init_object(self, cl, d, setProp = {}):
        if d["graph"] is None and d["r"] is not None and d["s"] is not None:
            try:
                r = self._db_read(cl, query = {"spx_r": d["r"],
                                               "spx_s": d["s"]}, kargs = d)
                d["zooid"] = r["zooid"]
            except KeyError as ex:
                if not d["store"]:
                    raise ex
                if not isinteger(d["r"]) or not isinteger(d["s"]):
                    raise TypeError("r and s must be positive integers")
                if d["r"] < 1 or d["s"] < 1:
                    raise ValueError("r and s must be positive integers")
                c = [tuple(x) for x
                     in cartesian_product([[tuple(y) for y
                                            in Integers(2)**Integer(d["s"])],
                                           Integers(d["r"]),
                                           [Integer(1), Integer(-1)]])]
                if d["r"] == 1:
                    if d["multiedges"] is False:
                        raise ValueError("A SPX graph with r = 1 has multiple edges")
                    d["data"] = sum([[((v, n, s), (v, n, -s)),
                                      ((v, n, s),
                                       (v[1:] + (Integer(0),), n, -s)),
                                      ((v, n, s),
                                       (v[1:] + (Integer(1),), n, -s))]
                                     for v, n, s in c if s == 1], [])
                    d["multiedges"] = True
                else:
                    d["data"] = [c, spx_adj]
                self._construct_graph(d)
                d["data"] = None
        ZooGraph._init_object(self, cl, d, setProp = setProp)

    def _construct_object(self, cl, d):
        ZooGraph.__init__(self, **d)

        if d["r"] is not None and d["s"] is not None:
            assert(d["r"] * 2**(d["s"]+1) == self._graphprops["order"])
        if len(self._spxprops) == 0:
            try:
                self._db_read(cl, kargs = d)
            except KeyError as ex:
                if not d["store"]:
                    raise ex

    def _repr_generic(self):
        return "split Praeger-Xu(2, %d, %d) graph on %d vertices" \
                                % (self.spx_r(), self.spx_s(), self.order())

    def spx_r(self):
        return lookup(self._spxprops, "spx_r")

    def spx_s(self):
        return lookup(self._spxprops, "spx_s")

def spx_adj(x, y):
    xv, xn, xs = x
    yv, yn, ys = y
    if xs == ys:
        return False
    if xn == yn and xv == yv:
        return True
    if xn + xs != yn:
        return False
    if xs == -1:
        xv, yv = yv, xv
    return xv[1:] == yv[:-1]

info = ZooInfo(SPXGraph)
