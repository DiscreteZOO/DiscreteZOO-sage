from sage.rings.integer import Integer
from ..zooentity import ZooInfo
from ..zoograph import ZooGraph
from ..zoograph import import_graphs
from ..zooobject import ZooObject
from ...db.query import Table
from ...util.utility import isinteger
from ...util.utility import lookup

class VTGraph(ZooGraph):
    _vtprops = None
    _parent = ZooGraph
    _spec = None
    _dict = "_vtprops"

    def __init__(self, data = None, index = None, symcubic_index = None,
                 **kargs):
        ZooObject._init_(self, VTGraph, kargs,
                         defNone = ["order"],
                         setVal = {"data": data, "vt_index": index},
                         setProp = {"vt_index": "vt_index"})

    def _parse_params(self, d):
        if isinteger(d["data"]):
            if d["vt_index"] is None:
                d["zooid"] = Integer(d["data"])
            else:
                d["order"] = Integer(d["data"])
            d["data"] = None
            return True
        else:
            return ZooGraph._parse_params(self, d)

    def _construct_object(self, cl, d):
        if d["order"] is not None:
            if d["vt_index"] is not None:
                join = Table(cl._spec["name"]).join(
                                    Table(ZooGraph._spec["name"]),
                                    by = frozenset([cl._spec["primary_key"]]))
                try:
                    r = self._db_read(ZooGraph, join,
                                      {"order": d["order"],
                                       "vt_index": d["vt_index"]}, kargs = d)
                    d["zooid"] = r["zooid"]
                    d["graph"] = None
                except KeyError:
                    pass
        ZooGraph.__init__(self, **d)

        if d["order"] is not None:
            assert(d["order"] == self._graphprops["order"])
        if len(self._vtprops) == 0:
            try:
                self._db_read(cl, kargs = d)
            except KeyError as ex:
                if not d["store"]:
                    raise ex

    def _repr_generic(self):
        if lookup(self._graphprops, "connected_components_number",
                  default = 1) > 1:
            out = "disconnected "
        else:
            out = ""
        out += "vertex-transitive %s" % ZooGraph._repr_generic(self)
        index = self.vt_index()
        if index is not None:
            out = "%s, number %s" % (out, index)
        return out

    def vt_index(self):
        return lookup(self._vtprops, "vt_index", default = None)

def import_vt(file, db = None, format = "sparse6", index = "index",
              verbose = False):
    import_graphs(file, cl = VTGraph, db = db, format = format, index = index,
                  verbose = verbose)

info = ZooInfo(VTGraph)
