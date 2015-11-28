__all__ = ['fields', 'ZooGraph', 'info']

from sage.graphs.graph import GenericGraph
from sage.graphs.graph import Graph
from sage.rings.integer import Integer
from sage.rings.rational import Rational
from sage.rings.real_mpfr import RealNumber
from hashlib import sha256
from types import MethodType
from ..query import Value
from ..utility import construct
from ..utility import default
from ..utility import isinteger
from ..utility import lookup
from ..utility import update
from ..zooobject import ZooInfo
from ..zooobject import ZooObject

_objspec = {
    "name": "graph",
    "dict": "_props",
    "primary_key": "id",
    "indices": {"average_degree", "order"},
    "skip": {"id", "data", "unique_id"},
    "fields" : {
        #"automorphism_group": ZooGroup,
        "average_degree": Rational,
        "average_distance": Rational,
        "chromatic_index": Integer,
        "chromatic_number": Integer,
        "clique_number": Integer,
        "cluster_transitivity": Rational,
        "clustering_average": Rational,
        "connected_components_number": Integer,
        "data": (str, {"not_null"}),
        "density": Rational,
        "diameter": Integer,
        "edge_connectivity": Integer,
        "fractional_chromatic_index": Integer,
        "genus": Integer,
        "girth": Integer,
        "has_multiple_edges": bool,
        "id": (Integer, {"autoincrement"}),
        "is_arc_transitive": bool,
        "is_asteroidal_triple_free": bool,
        "is_bipartite": bool,
        "is_cartesian_product": bool,
        "is_cayley": bool,
        "is_chordal": bool,
        "is_circulant": bool,
        "is_circular_planar": bool,
        "is_distance_regular": bool,
        "is_distance_transitive": bool,
        "is_edge_transitive": bool,
        "is_eulerian": bool,
        "is_even_hole_free": bool,
        "is_forest": bool,
        "is_gallai_tree": bool,
        "is_hamiltonian": bool,
        "is_interval": bool,
        "is_line_graph": bool,
        "is_long_antihole_free": bool,
        "is_long_hole_free": bool,
        "is_odd_hole_free": bool,
        "is_overfull": bool,
        "is_perfect": bool,
        "is_planar": bool,
        "is_prime": bool,
        "is_regular": bool,
        "is_split": bool,
        "is_strongly_regular": bool,
        "is_tree": bool,
        "is_vertex_transitive": bool,
        "lovasz_theta": RealNumber,
        "maximum_average_degree": Rational,
        "name": (str, {"unique"}),
        "number_of_loops": Integer,
        "odd_girth": Integer,
        "order": Integer,
        "radius": Integer,
        "size": Integer,
        "spanning_trees_count": Integer,
        "szeged_index": Integer,
        "triangles_count": Integer,
        "treewidth": Integer,
        "unique_id": (str, {"unique"}),
        "vertex_connectivity": Integer,
        "wiener_index": Integer,
        "zagreb1_index": Integer,
        "zagreb2_index": Integer
    },
    "compute": {"_props": {"diameter", "girth", "has_multiple_edges", "name",
                           "number_of_loops", "order", "size"}},
    "default": {}
}

class ZooGraph(Graph, ZooObject):
    _props = None
    _spec = _objspec

    def __init__(self, data = None, **kargs):
        cl = ZooGraph
        self._init_(cl, kargs, defNone = ["vertex_labels"],
                    setVal = {"data": data,
                              "immutable": True,
                              "data_structure": "static_sparse"})

        self._zooid = kargs["zooid"]
        if kargs["data"] is None:
            kargs["data"] = self._db_read(cl)["data"]
        propname = lookup(self._props, "name", default = None)
        if kargs["name"]:
            self._props["name"] = kargs["name"]
        elif propname:
            kargs["name"] = propname
        if propname == '':
            del self._props["name"]
        if kargs["vertex_labels"] is not None:
            kargs["data"] = Graph(kargs["data"]).relabel(kargs["vertex_labels"],
                                                         inplace = False)
        if kargs["loops"] is None:
            kargs["loops"] = self._props["number_of_loops"] > 0
        if kargs["multiedges"] is None:
            kargs["multiedges"] = self._props["has_multiple_edges"]
        construct(Graph, self, kargs)
        if kargs["cur"] is not None:
            self._db_write(cl, kargs["cur"])

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

    def _init_graph(self, cl, d, setProp = {}):
        if not isinstance(d["graph"], GenericGraph):
            raise TypeError("not a graph")
        if d["name"] is None:
            d["name"] = d["graph"].name()
        if isinstance(d["graph"], ZooGraph):
            d["zooid"] = d["graph"]._zooid
            c = cl
            while c is not None:
                if isinstance(d["graph"], c):
                    self._setprops(c, d["graph"]._getprops(c))
                c = c._parent
        if d["cur"] is not None:
            self._compute_props(cl, d)
            for k, v in setProp.items():
                self._getprops(cl)[k] = d[v]
        elif d["zooid"] is None:
            uid = unique_id(d["graph"])
            d["props"] = next(ZooInfo(cl).props(fields.unique_id == Value(uid),
                                                cur = d["cur"]))
        self._init_props(cl, d)
        d["data"] = d["graph"]
        d["graph"] = None

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
        if isinstance(attr, MethodType) and \
                attr.func_globals["__package__"].startswith("sage."):
            cl = type(self)
            while cl is not None:
                if name in cl._spec["fields"] and name not in cl._spec["skip"]:
                    _graphattr.func_name = name
                    _graphattr.func_doc = attr.func_doc
                    return _graphattr
                cl = cl._parent
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
