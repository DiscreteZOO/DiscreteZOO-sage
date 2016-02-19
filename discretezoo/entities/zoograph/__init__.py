__all__ = ['fields', 'ZooGraph', 'info']

from .zoograph import *
from ..zootypes import init_class
import fields

init_class(ZooGraph, fields)
fields.degree = fields.average_degree
fields.density = 2 * fields.size / (fields.order * (fields.order - 1))
fields.has_loops = fields.number_of_loops != 0
fields.is_connected = fields.connected_components_number <= 1
fields.is_half_transitive = fields.is_edge_transitive & \
                            fields.is_vertex_transitive & \
                            ~fields.is_arc_transitive
fields.is_semi_symmetric = fields.is_regular & \
                           fields.is_edge_transitive & \
                           ~fields.is_vertex_transitive
fields.is_triangle_free = fields.triangles_count == 0
fields.is_weakly_chordal = fields.is_long_hole_free & \
                           fields.is_long_antihole_free
