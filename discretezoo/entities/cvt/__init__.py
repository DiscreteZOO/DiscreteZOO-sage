r"""
CVTGraph module

Contains a class representing cubic vertex-transitive graphs.
"""
__all__ = ['fields', 'CVTGraph', 'info']

from .cvtgraph import *
from ..zootypes import init_class
import fields

init_class(CVTGraph, fields)
