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
    "indices": {"average_degree", "vertices"},
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
        "is_cayley_graph": bool,
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
        "is_weakly_chordal": bool,
        "lovasz_theta": RealNumber,
        "maximum_average_degree": Rational,
        "name": str,
        "num_edges": Integer,
        "number_of_loops": Integer,
        "odd_girth": Integer,
        "radius": Integer,
        "spanning_trees_count": Integer,
        "szeged_index": Integer,
        "triangles_count": Integer,
        "treewidth": Integer,
        "vertex_connectivity": Integer,
        "vertices": Integer,
        "wiener_index": Integer,
        "zagreb1_index": Integer,
        "zagreb2_index": Integer
    }
}

class ZooGraph(Graph, ZooObject):
    _props = None
    _spec = _objspec
    
    def __init__(self, data = None, zooid = None, props = None, graph = None,
                 name = None, cur = None, db = None, **kargs):
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
                self._props["vertices"] = graph.order()
            elif zooid is None:
                raise IndexError("graph id not given")
        else:
            cur = None
            if props is not None:
                self._props = self._todict(props,
                                           skip = ZooGraph._spec["skip"],
                                           fields = ZooGraph._spec["fields"])

        ZooObject.__init__(self, db)
        self._zooid = zooid
        if data is None:
            data = self._db_read()["data"]
        Graph.__init__(self, data = data, name = name, **kargs)
        if cur is not None:
            self._db_write(cur)

    def copy(self, weighted = None, implementation = 'c_graph',
             data_structure = None, sparse = None, immutable = None):
        if immutable is False or (data_structure is not None
                                  and data_structure is not'static_sparse'):
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
                            cur = cur)

    def load_db_data(self):
        self._db_read()

    def average_degree(self, store = False, **kargs):
        default = len(kargs) == 0
        try:
            if not default:
                raise NotImplementedError
            return lookup(self._props, "average_degree")
        except (KeyError, NotImplementedError):
            d = Graph.average_degree(self, **kargs)
            if default and store:
                update(self._props, "average_degree", d)
            return d

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

info = ZooInfo(ZooGraph)
