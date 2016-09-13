r"""
SPXGraph module

Contains a class representing split Praeger-Xu graphs.
"""
__all__ = ['fields', 'SPXGraph', 'info']

from .spxgraph import *
from ..zootypes import init_class
import fields

init_class(SPXGraph, fields)
