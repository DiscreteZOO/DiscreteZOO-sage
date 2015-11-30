from sage.graphs.graph import GenericGraph
from sage.graphs.graph import Graph
from sage.rings.integer import Integer
from hashlib import sha256
from types import MethodType
from ..query import Column
from ..query import Value
from ..utility import construct
from ..utility import default
from ..utility import isinteger
from ..utility import lookup
from ..utility import update
from ..zooobject import ZooInfo
from ..zooobject import ZooObject

class ZooGraph(Graph, ZooObject):
    _props = None
    _spec = None

    def __init__(self, data = None, **kargs):
        ZooObject.__init__(self, ZooGraph, kargs, defNone = ["vertex_labels"],
                           setVal = {"data": data,
                                     "immutable": True,
                                     "data_structure": "static_sparse"})

    def _init_defaults(self, d):
        default(d, "zooid")
        default(d, "props")
        default(d, "graph")
        default(d, "name")
        default(d, "cur")
        default(d, "loops")
        default(d, "multiedges")

    def _parse_params(self, d):
        if isinteger(d["data"]):
            d["zooid"] = Integer(d["data"])
            d["data"] = None
            return True
        else:
            return False

    def _init_params(self, d):
        if isinstance(d["data"], GenericGraph):
            d["graph"] = d["data"]
            d["data"] = None
        elif isinstance(d["data"], dict):
            d["props"] = d["data"]
            d["data"] = None

    def _init_skip(self, d):
        if d["props"] is not None:
            if "id" in d["props"]:
                d["zooid"] = d["props"]["id"]
                del d["props"]["id"]
            if "data" in d["props"]:
                d["data"] = d["props"]["data"]
                del d["props"]["data"]

    def _clear_params(self, d):
        pass

    def _init_object(self, cl, d, setProp = {}):
        if d["graph"] is not None:
            self._init_graph(cl, d, setProp)
            cl._clear_params(self, d)
        else:
            d["cur"] = None
            self._init_props(cl, d)
        cl._construct_object(self, cl, d)

    def _init_graph(self, cl, d, setProp = {}):
        if not isinstance(d["graph"], GenericGraph):
            raise TypeError("not a graph")
        if d["name"] is None:
            d["name"] = d["graph"].name()
        if isinstance(d["graph"], ZooGraph):
            d["zooid"] = d["graph"]._zooid
            self._copy_props(cl, d["graph"])
        if d["cur"] is not None:
            self._compute_props(cl, d)
            for k, v in setProp.items():
                self._getprops(cl)[k] = d[v]
        elif d["zooid"] is None:
            uid = unique_id(d["graph"])
            d["props"] = next(ZooInfo(cl).props(Column("unique_id") == Value(uid),
                                                cur = d["cur"]))
        self._init_props(cl, d)
        d["data"] = d["graph"]
        d["graph"] = None

    def _construct_object(self, cl, d):
        self._zooid = d["zooid"]
        if d["data"] is None:
            d["data"] = self._db_read(cl)["data"]
        propname = lookup(self._props, "name", default = None)
        if d["name"]:
            self._props["name"] = d["name"]
        elif propname:
            d["name"] = propname
        if propname == '':
            del self._props["name"]
        if d["vertex_labels"] is not None:
            d["data"] = Graph(d["data"]).relabel(d["vertex_labels"],
                                                 inplace = False)
        if d["loops"] is None:
            d["loops"] = self._props["number_of_loops"] > 0
        if d["multiedges"] is None:
            d["multiedges"] = self._props["has_multiple_edges"]
        construct(Graph, self, d)

    def _repr_generic(self):
        name = ""
        if self.allows_loops():
            name += "looped "
        if self.allows_multiple_edges():
            name += "multi-"
        if self._directed:
            name += "di"
        name += "graph on %d vert"%self.order()
        if self.order() == 1:
            name += "ex"
        else:
            name += "ices"
        return name

    def _repr_(self):
        name = self._repr_generic()
        if self.name() != '':
            name = self.name() + ": " + name
        else:
            name = name.capitalize()
        return name

    def __getattribute__(self, name):
        def _graphattr(*largs, **kargs):
            store = lookup(kargs, "store", default = False, destroy = True)
            default = len(largs) + len(kargs) == 0
            try:
                if not default:
                    raise NotImplementedError
                return lookup(self._props, name)
            except (KeyError, NotImplementedError):
                a = Graph.__getattribute__(self, name)(*largs, **kargs)
                if default and store:
                    update(self._props, name, a)
                return a
        attr = Graph.__getattribute__(self, name)
        try:
            if isinstance(attr, MethodType) and \
                    attr.func_globals["__package__"].startswith("sage."):
                cl = type(self)
                while cl is not None:
                    if name in cl._spec["fields"] and \
                            name not in cl._spec["skip"]:
                        _graphattr.func_name = name
                        _graphattr.func_doc = attr.func_doc
                        return _graphattr
                    cl = cl._parent
        except AttributeError:
            pass
        return attr

    def copy(self, weighted = None, implementation = 'c_graph',
             data_structure = None, sparse = None, immutable = None):
        if immutable is False or (data_structure is not None
                                  and data_structure is not 'static_sparse'):
            return Graph(self).copy(weighted = weighted,
                                    implementation = implementation,
                                    data_structure = data_structure,
                                    sparse = sparse,
                                    immutable = immutable)
        else:
            return Graph.copy(self, weighted = weighted,
                                    implementation = implementation,
                                    data_structure = data_structure,
                                    sparse = sparse,
                                    immutable = immutable)

    def relabel(self, perm = None, inplace = True, return_map = False,
                check_input = True, complete_partial_function = True,
                immutable = True):
        if inplace:
            raise ValueError("To relabel an immutable graph use inplace=False")
        G = Graph(self, immutable = False)
        perm = G.relabel(perm, return_map = True, check_input = check_input,
                         complete_partial_function = complete_partial_function)
        if immutable is not False:
            G = self.__class__(self, vertex_labels = perm)
        if return_map:
            return G, perm
        else:
            return G

    def data(self):
        try:
            return lookup(self._props, "data")
        except (KeyError, TypeError):
            return data(self)

    def unique_id(self):
        try:
            return lookup(self._props, "unique_id")
        except (KeyError, TypeError):
            return unique_id(self)

    def is_regular(self, k = None, store = False, **kargs):
        default = len(kargs) == 0
        try:
            if not default:
                raise NotImplementedError
            r = lookup(self._props, "is_regular")
            return r and (True if k is None
                          else k == self.average_degree(store = store))
        except (KeyError, NotImplementedError):
            r = Graph.is_regular(self, k, **kargs)
            if default and store:
                update(self._props, "is_regular", r)
                if r and k is not None:
                    update(self._props, "average_degree", k)
            return r
    is_regular.func_doc = Graph.is_regular.func_doc

def canonical_label(graph):
    return graph.canonical_label(algorithm = "sage")

def data(graph):
    # TODO: determine the most appropriate way of representing the graph
    return canonical_label(graph).sparse6_string()

def unique_id(graph):
    return sha256(data(graph)).hexdigest()

info = ZooInfo(ZooGraph)
