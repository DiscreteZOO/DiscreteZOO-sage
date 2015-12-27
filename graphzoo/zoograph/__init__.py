__all__ = ['fields', 'ZooGraph', 'info']

from .zoograph import *
from ..zootypes import init_class
import fields

init_class(ZooGraph, fields)
