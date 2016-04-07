r"""
Database interface

This module defines an abstract class for database interfaces.
"""

from sage.rings.real_mpfr import RealNumber
import discretezoo
from ..entities.zooentity import ZooEntity
from ..util.utility import lookup

class DB:
    r"""
    An abstract class for database interfaces.
    """
    convert_to = None
    convert_from = None
    track = discretezoo.TRACK_CHANGES

    def __init__(self, *largs, **kargs):
        r"""
        Object constructor.

        INPUT:

        - ``track`` - whether to track changes to a database (must be a named
          parameter; default: ``discretezoo.TRACK_CHANGES``).

        - any other parameter will be passed to the ``connect`` method.
        """
        self.track = lookup(kargs, "track",
                            default = discretezoo.TRACK_CHANGES,
                            destroy = True)
        self.connect(*largs, **kargs)

    def connect(self, *largs, **kargs):
        r"""
        Connect to the database.

        Not implemented, to be overridden.
        """
        raise NotImplementedError

    def cursor(self, *largs, **kargs):
        r"""
        Return a cursor.

        Not implemented, to be overridden.
        """
        raise NotImplementedError

    def commit(self, *largs, **kargs):
        r"""
        Commit the current transaction.

        Not implemented, to be overridden.
        """
        raise NotImplementedError

    def rollback(self, *largs, **kargs):
        r"""
        Rollback the current transaction.

        Not implemented, to be overridden.
        """
        raise NotImplementedError

    def init_table(self, *largs, **kargs):
        r"""
        Initialize a table.

        Not implemented, to be overridden.
        """
        raise NotImplementedError

    def insert_row(self, *largs, **kargs):
        r"""
        Insert a row.

        Not implemented, to be overridden.
        """
        raise NotImplementedError

    def query(self, *largs, **kargs):
        r"""
        Perform a query.

        Not implemented, to be overridden.
        """
        raise NotImplementedError

    def to_db_type(self, x):
        r"""
        Convert ``x`` to a type suitable for database communication.

        INPUT:

        - ``x`` - the object to be converted.
        """
        if isinstance(x, ZooEntity):
            return self.convert_to[ZooEntity](x._zooid)
        elif isinstance(x, RealNumber):
            return self.convert_to[RealNumber](x)
        elif type(x) in self.convert_to:
            return self.convert_to[type(x)](x)
        else:
            return x

    def from_db_type(self, x, t):
        r"""
        Convert ``x`` to type ``t``.

        INPUT:

        - ``x`` - the object to be converted.

        - ``t`` - the type to convert to.
        """
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
