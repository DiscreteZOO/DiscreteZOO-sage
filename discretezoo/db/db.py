from sage.rings.real_mpfr import RealNumber
import discretezoo
from ..entities.zooentity import ZooEntity
from ..util.utility import lookup

class DB:
    convert_to = None
    convert_from = None
    track = discretezoo.TRACK_CHANGES

    def __init__(self, *largs, **kargs):
        self.track = lookup(kargs, "track",
                            default = discretezoo.TRACK_CHANGES,
                            destroy = True)
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
        if isinstance(x, ZooEntity):
            return self.convert_to[ZooEntity](x._zooid)
        elif isinstance(x, RealNumber):
            return self.convert_to[RealNumber](x)
        elif type(x) in self.convert_to:
            return self.convert_to[type(x)](x)
        else:
            return x

    def from_db_type(self, x, t):
        if isinstance(t, tuple):
            t = t[0]
        if issubclass(t, ZooEntity):
            return self.convert_from[ZooEntity](x)
        elif t in self.convert_from:
            return self.convert_from[t](x)
        else:
            return x

    def __repr__(self):
        return "<database object at 0x%08x: %s>" % (id(self), str(self))
