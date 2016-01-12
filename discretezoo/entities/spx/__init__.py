__all__ = ['fields', 'SPXGraph', 'info']

from .spxgraph import *
from ..zootypes import init_class
import fields

init_class(SPXGraph, fields)
