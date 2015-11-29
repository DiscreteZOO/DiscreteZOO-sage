from . import objspec
from ..query import Column
from ..cvt.fields import *

for _k in objspec["fields"]:
    exec('%s = Column(%s)' % (_k, repr(_k)))
del _k
del objspec
