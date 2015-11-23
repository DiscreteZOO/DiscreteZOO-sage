__all__ = ['fields', 'ZooGraph', 'info']

from sage.graphs.graph import GenericGraph
from sage.graphs.graph import Graph
from sage.rings.integer import Integer
from sage.rings.rational import Rational
from sage.rings.real_mpfr import RealNumber
from hashlib import sha256
from types import MethodType
from ..query import Table
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
    "skip": {"id", "data"},
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
                           "number_of_loops", "order", "size", "unique_id"}},
    "default": {}
}

class ZooGraph(Graph, ZooObject):
    _props = None
    _spec = _objspec

    def __init__(self, data = None, zooid = None, props = None, graph = None,
                 vertex_labels = None, name = None, cur = None, db = None,
                 **kargs):
        ZooObject.__init__(self, db)
        cl = ZooGraph
        kargs["immutable"] = True
        kargs["data_structure"] = "static_sparse"
        if isinteger(data):
            zooid = Integer(data)
            data = None
        else:
            data, props, graph, zooid = self._init_params(data, props, graph,
                                                          zooid)

        if graph is not None:
            data, name, zooid = self._init_graph(cl, graph, name, cur, zooid)
        else:
            cur = None
            props = self._init_props(cl, props)
        self._zooid = zooid
        if data is None:
            data = self._db_read(cl)["data"]
        propname = lookup(self._props, "name", default = None)
        if name:
            self._props["name"] = name
        elif propname:
            name = propname
        if vertex_labels is not None:
            data = Graph(data).relabel(vertex_labels, inplace = False)
        kargs["loops"] = self._props["number_of_loops"] > 0
        kargs["multiedges"] = self._props["has_multiple_edges"]
        Graph.__init__(self, data = data, name = name, **kargs)
        if cur is not None:
            self._db_write(cl, cur)

    def _init_params(self, data, props, graph, zooid):
        if isinstance(data, GenericGraph):
            graph = data
            data = None
        elif isinstance(data, dict):
            props = data
            data = None
        if props is not None:
            if "id" in props:
                zooid = props["id"]
            if "data" in props:
                data = props["data"]
            props = {k: v for k, v in props.items() if k not in ["id", "data"]}
        return (data, props, graph, zooid)

    def _init_graph(self, cl, graph, name, cur, zooid):
        if not isinstance(graph, GenericGraph):
            raise TypeError("not a graph")
        if name is None:
            name = graph.name()
        if isinstance(graph, ZooGraph):
            zooid = graph._zooid
            c = cl
            while c is not None:
                if isinstance(graph, c):
                    self._setprops(c, graph._getprops(c))
                c = c._parent
        if cur is not None:
            c = cl
            while c is not None:
                if self._getprops(c) is None:
                   self._setprops(c, {})
                c = c._parent
            for f, d in cl._spec["default"].items():
                props = self.__getattribute__(f)
                for k, v in d.items():
                    props[k] = v
            for f, s in cl._spec["compute"].items():
                props = self.__getattribute__(f)
                for k in s:
                    try:
                        lookup(props, k)
                    except KeyError:
                        props[k] = graph.__getattribute__(k)()
        elif zooid is None:
            raise IndexError("graph id not given")
        return (graph, name, zooid)

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
                not attr.func_globals["__package__"].startswith("graphzoo."):
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
            # TODO: determine the most appropriate way of representing the graph
            return self.sparse6_string()

    def unique_id(self):
        try:
            return lookup(self._props, "unique_id")
        except (KeyError, TypeError):
            return sha256(self.sparse6_string()).hexdigest()

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

info = ZooInfo(ZooGraph)
