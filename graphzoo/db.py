from sage.rings.real_mpfr import RealNumber
from zooobject import ZooObject

class DB:
    convert_to = None
    convert_from = None

    def __init__(self, *largs, **kargs):
        self.connect(*largs, **kargs)

    def connect(self, **kargs):
        raise NotImplementedError

    def cursor(self, **kargs):
        raise NotImplementedError

    def commit(self, **kargs):
        raise NotImplementedError

    def rollback(self, **kargs):
        raise NotImplementedError

    def init_table(self, **kargs):
        raise NotImplementedError

    def insert_row(self, **kargs):
        raise NotImplementedError

    def query(self, **kargs):
        raise NotImplementedError

    def to_db_type(self, x):
        if isinstance(x, ZooObject):
            return self.convert_to[ZooObject](x._zooid)
        elif isinstance(x, RealNumber):
            return self.convert_to[RealNumber](x)
        elif type(x) in self.convert_to:
            return self.convert_to[type(x)](x)
        else:
            return x

    def from_db_type(self, x, t):
        if isinstance(t, tuple):
            t = t[0]
        if issubclass(t, ZooObject):
            return self.convert_from[ZooObject](x)
        elif t in self.convert_from:
            return self.convert_from[t](x)
        else:
            return x
