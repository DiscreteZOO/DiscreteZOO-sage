from . import _objspec
from ..query import Column

for _k in _objspec["fields"]:
    exec('%s = Column(%s)' % (_k, repr(_k)))
del _k
