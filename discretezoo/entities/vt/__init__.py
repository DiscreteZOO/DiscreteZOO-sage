__all__ = ['fields', 'VTGraph', 'info']

from .vtgraph import *
from ..zootypes import init_class
import fields

init_class(VTGraph, fields)
