r"""
ZooObject module

Contains a superclass for all DiscreteZOO objects.
"""
__all__ = ['fields', 'ZooObject', 'info']

from .zooobject import *
from ..zootypes import init_class
import fields

init_class(ZooObject, fields)
