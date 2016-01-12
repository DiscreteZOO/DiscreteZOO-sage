__all__ = ['fields', 'ZooObject', 'info']

from .zooobject import *
from ..zootypes import init_class
import fields

init_class(ZooObject, fields)
