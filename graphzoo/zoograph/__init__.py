__all__ = ['fields', 'ZooGraph', 'info']

from sage.rings.integer import Integer
from sage.rings.rational import Rational
from sage.rings.real_mpfr import RealNumber
from ..query import makeFields
from ..zooobject import ZooObject
from .zoograph import *
import fields

objspec = {
    "name": "graph",
    "primary_key": "zooid",
    "indices": {"average_degree", "order"},
    "skip": {"data", "zooid"},
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
        "vertex_connectivity": Integer,
        "wiener_index": Integer,
        "zagreb1_index": Integer,
        "zagreb2_index": Integer,
        "zooid": ZooObject
    },
    "compute": {ZooGraph: {"diameter", "girth", "has_multiple_edges",
                           "name", "number_of_loops", "order", "size"}},
    "default": {}
}

ZooGraph._spec = objspec
makeFields(ZooGraph, fields)
