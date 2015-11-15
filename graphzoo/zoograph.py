from sage.graphs.graph import GenericGraph
from sage.graphs.graph import Graph
from sage.rings.integer import Integer
from sage.rings.rational import Rational
from sage.rings.real_mpfr import RealNumber
from query import Table
from utility import isinteger
from utility import lookup
from utility import update
from zooobject import ZooInfo
from zooobject import ZooObject

_objspec = {
    "name": "graph",
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
        "name": str,
        "number_of_loops": Integer,
        "odd_girth": Integer,
        "order": Integer,
        "radius": Integer,
        "size": Integer,
        "spanning_trees_count": Integer,
        "szeged_index": Integer,
        "triangles_count": Integer,
        "treewidth": Integer,
        "vertex_connectivity": Integer,
        "wiener_index": Integer,
        "zagreb1_index": Integer,
        "zagreb2_index": Integer
    },
    "special": {"is_regular"}
}

class ZooGraph(Graph, ZooObject):
    _props = None
    _spec = _objspec
    
    def __init__(self, data = None, zooid = None, props = None, graph = None,
                 vertex_labels = None, name = None, cur = None, db = None,
                 **kargs):
        ZooObject.__init__(self, db)
        kargs["immutable"] = True
        kargs["data_structure"] = "static_sparse"
        if isinteger(data):
            zooid = Integer(data)
            data = None
        elif isinstance(data, GenericGraph):
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

        if graph is not None:
            if not isinstance(graph, GenericGraph):
                raise TypeError("not a graph")
            data = graph
            if name is None:
                name = graph.name()
            if isinstance(graph, ZooGraph):
                zooid = graph._zooid
                self._props = graph._props
            if cur is not None:
                if self._props is None:
                    self._props = {}
                self._props["diameter"] = graph.diameter()
                self._props["girth"] = graph.girth()
                self._props["order"] = graph.order()
            elif zooid is None:
                raise IndexError("graph id not given")
        else:
            cur = None
            if props is not None:
                self._props = self._todict(props,
                                           skip = ZooGraph._spec["skip"],
                                           fields = ZooGraph._spec["fields"])

        self._zooid = zooid
        if data is None:
            data = self._db_read()["data"]
        if vertex_labels is not None:
            data = Graph(data).relabel(vertex_labels, inplace = False)
        Graph.__init__(self, data = data, name = name, **kargs)
        if cur is not None:
            self._db_write(cur)

    def __getattribute__(self, name):
        def _graphattr(store = False, *largs, **kargs):
            default = len(largs) + len(kargs) == 0
            try:
                if not default:
                    raise NotImplementedError
                return lookup(self._props, name)
            except (KeyError, NotImplementedError):
                a = Graph.__getattribute__(self, name)(*largs, **kargs)
                if default and store:
                    update(self._props, attr, d)
                return a
        attr = Graph.__getattribute__(self, name)
        cl = type(self)
        while cl is not None:
            if name in cl._spec["fields"] and name not in cl._spec["skip"] \
                    and name not in cl._spec["special"]:
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

    def _db_read(self, join = None, query = None):
        if query is None:
            if self._zooid is None:
                raise IndexError("graph id not given")
            query = {"id": self._zooid}
        t = Table(ZooGraph._spec["name"])
        if join is None:
            join = t
        cur = self._db.query([t], join, query)
        r = cur.fetchone()
        cur.close()
        if r is None:
            raise KeyError(query)
        self._props = self._todict(r, skip = ZooGraph._spec["skip"],
                                   fields = ZooGraph._spec["fields"])
        return r

    def _db_write(self, cur):
        self._db.insert_row(ZooGraph._spec["name"],
                            dict(self._props.items() + \
                                 [("id", self._zooid),
                                  ("data", self.sparse6_string())]),
                            cur = cur, id = ZooGraph._spec["primary_key"])
        self._zooid = self._db.lastrowid(cur)

    def load_db_data(self):
        self._db_read()

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
