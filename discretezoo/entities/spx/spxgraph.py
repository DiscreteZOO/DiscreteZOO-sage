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

    def _construct_object(self, cl, d):
        if d["r"] is not None and d["s"] is not None:
            r = self._db_read(cl, query = {"spx_r": d["r"],
                                           "spx_s": d["s"]})
            d["zooid"] = r["zooid"]
            d["graph"] = None
        ZooGraph.__init__(self, **d)

        if d["r"] is not None and d["s"] is not None:
            assert(d["r"] * 2**(d["s"]+1) == self._graphprops["order"])
        if len(self._spxprops) == 0:
            self._db_read(cl)

    def _repr_generic(self):
        return "split Praeger-Xu(2, %d, %d) graph on %d vertices" \
                                % (self.spx_r(), self.spx_s(), self.order())

    def spx_r(self):
        return lookup(self._spxprops, "spx_r")

    def spx_s(self):
        return lookup(self._spxprops, "spx_s")

info = ZooInfo(SPXGraph)
