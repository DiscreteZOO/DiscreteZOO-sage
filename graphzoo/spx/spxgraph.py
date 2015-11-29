from sage.rings.integer import Integer
from ..utility import isinteger
from ..utility import lookup
from ..zoograph import ZooGraph
from ..zooobject import ZooInfo
from ..zooobject import ZooObject

class SPXGraph(ZooGraph):
    _spxprops = None
    _parent = ZooGraph
    _spec = None

    def __init__(self, data = None, s = None, **kargs):
        cl = SPXGraph
        ZooObject.__init__(self, cl, kargs, defNone = ["r"],
                           setVal = {"data": data, "s": s},
                           setProp = {"spx_r": "r", "spx_s": "s"})

        if kargs["r"] is not None and kargs["s"] is not None:
            r = self._db_read(cl, query = {"spx_r": kargs["r"],
                                           "spx_s": kargs["s"]})
            kargs["zooid"] = r["id"]
            kargs["graph"] = None
        ZooGraph.__init__(self, **kargs)

        if kargs["r"] is not None and kargs["s"] is not None:
            assert(kargs["r"] * 2**(kargs["s"]+1) == self._props["order"])
        if self._spxprops is None:
            self._db_read(cl)
        if kargs["cur"] is not None:
            self._db_write(cl, kargs["cur"])

    def _parse_params(self, d):
        if isinteger(d["data"]):
            if d["s"] is None:
                d["zooid"] = Integer(d["data"])
            else:
                d["r"] = Integer(d["data"])
            d["data"] = None
            return True
        else:
            return False

    def _clear_params(self, d):
        d["r"] = None
        d["s"] = None

    def _repr_(self):
        name = "Split Praeger-Xu(2, %d, %d) graph on %d vertices" \
                                % (self.spx_r(), self.spx_s(), self.order())
        if self.name() != '':
            name = self.name() + ": " + name
        return name

    def spx_r(self):
        return lookup(self._spxprops, "spx_r")

    def spx_s(self):
        return lookup(self._spxprops, "spx_s")

info = ZooInfo(SPXGraph)
