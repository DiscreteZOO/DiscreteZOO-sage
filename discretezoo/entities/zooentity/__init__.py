r"""
ZooEntity module

Contains a superclass for all DiscreteZOO entities.
"""
__all__ = ['ZooEntity', 'ZooInfo']

from .zooentity import *
from ..zootypes import init_class

init_class(ZooEntity)

from ..zoodict import ZooDict
from ..zooset import ZooSet
