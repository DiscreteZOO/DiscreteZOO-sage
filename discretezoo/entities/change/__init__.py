r"""
Change module

Contains a class representing changes to the database.
"""
__all__ = ['Change']

from ..zootypes import init_class
from .change import *

init_class(Change)
