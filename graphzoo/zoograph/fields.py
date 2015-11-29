from . import objspec
from ..query import Column

for _k in objspec["fields"]:
    exec('%s = Column(%s)' % (_k, repr(_k)))
del _k
del objspec
