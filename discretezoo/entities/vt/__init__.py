r"""
VTGraph module

Contains a class representing vertex-transitive graphs.
"""
__all__ = ['fields', 'VTGraph', 'info']

from .vtgraph import *
from ..zootypes import init_class
import fields

init_class(VTGraph, fields)
