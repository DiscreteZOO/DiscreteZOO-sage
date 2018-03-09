r"""
A set metaclass

This module provides a function to create set-like classes.
"""

import discretezoo
from .zooentity import ZooEntity
from .zooproperty import ZooProperty
from .zootypes import register_type
from ..db.query import Column
from ..db.query import ColumnSet
from ..db.query import Table
from ..db.query import enlist
from ..util.utility import lookup
from ..util.utility import to_json

class _ZooSet(dict, ZooProperty):
    r"""
    A generic set class with database interaction.

    Extends Python's ``dict`` class.
    """
    _parent = None
    _objid = None
    _use_tuples = None

    def __init__(self, data, vals = None, **kargs):
        r"""
        Object constructor.

        INPUT:

        - ``data`` - a ``_ZooSet`` instance to replicate, or the ID of the
          object for which properties are being represented.

        - ``vals`` - a set of values to be stored (default: ``None``).
          Only used if ``store`` is ``True``.

        - ``db`` -- the database being used (must be a named parameter;
          default: ``None``).

        - ``store`` - whether to store the data in ``vals`` to the database
          (must be a named parameter; default: ``discretezoo.WRITE_TO_DB``).

        - ``cur`` - the cursor to use for database interaction
          (must be a named parameter; default: ``None``).

        - ``commit`` - whether to commit the changes to the database
          (must be a named parameter; default: ``None``).
        """
        dict.__init__(self)
        self._zooid = False
        if isinstance(data, _ZooSet):
            self._objid = data._objid
            dict.update(self, data)
            default(kargs, "db", data._db)
            ZooProperty._init_(self, kargs)
        else:
            ZooProperty._init_(self, kargs)
            self._objid = data
            if vals is not None and kargs["store"]:
                for val in vals:
                    self.add(val, store = True, cur = kargs["cur"])
                if kargs["commit"]:
                    self._db.commit()
            else:
                t = Table(self._spec["name"])
                cur = self._db.query([t], t, {self._foreign_key: data,
                                              "deleted": False},
                                     cur = kargs["cur"])
                for r in cur:
                    v = tuple([r[k] for k in self._ordering])
                    if not self._use_tuples:
                        v = v[0]
                    self[v] = r[self._spec["primary_key"]]

    def __getattr__(self, name):
        r"""
        Get the set of values associated to the property with the given name.

        INPUT:

        - ``name`` - the name of the requested property.
        """
        if name == self._spec["primary_key"]:
            return set(self.values())
        if name not in self._ordering:
            raise AttributeError(name)
        if not self._use_tuples:
            return {t for t in self}
        i = self._ordering.index(name)
        return {t[i] for t in self}

    def __repr__(self):
        r"""
        The object representation of the set.
        """
        return '{%s}' % ', '.join(sorted(self))

    def _unique_index(self):
        r"""
        Return a list of columns uniquely determining a row in the database.

        Returns a list of value columns.
        """
        return self._spec["indices"][0][0]

    def _normalize(self, x, id = None):
        r"""
        Normalize a value to the standard form.

        Returns a triple containing the value in the standard and tuple forms
        and its entry ID.

        INPUT:

        - ``x`` - the value to normalize.

        - ``id`` - the entry ID of the value. If ``None`` (default), it is
          retrieved from the value.
        """
        tx = tuple(enlist(x))
        if id is None and len(tx) > len(self._ordering):
            id = tx[0]
            tx = tx[1:]
        if not self._use_tuples and len(tx) == 1:
            x = tx[0]
        else:
            x = tx
        return (x, tx, id)

    @staticmethod
    def _get_column(cl, name, table, join = None, by = None):
        r"""
        Return a ``ColumnSet`` object for a property of the given class.

        INPUT:

        - ``cl`` - the class to get the object for.

        - ``name`` - the name of the property. Ignored in this context.

        - ``table`` - the table containing the objects whose properties are
          represented by ``cl``.

        - ``join`` - a join of tables needed to determine the object
          (default: ``None``).

        - ``by`` - the criterion to join by (default: ``None``).
          See ``db.query.Table.join`` for more information.
        """
        if not isinstance(table, Table):
            table = Table(table)
        if join is not None:
            table = join.join(table, by = by)
        col = None if cl._use_tuples else cl._ordering[0]
        return ColumnSet(cl, col, join = table,
            by = (("deleted", False),
                  (cl._foreign_key,
                   Column(cl._foreign_obj._spec["primary_key"],
                          table = table))),
            foreign = cl._foreign_key, ordering = cl._ordering)

    def _to_json(self):
        r"""
        Return an object suitable for conversion to JSON.

        Returns a ``list`` with sorted values recursively converted
        to a suitable format.
        """
        return sorted(to_json(v) for v in self)

    def add(self, x, id = None, **kargs):
        r"""
        Add an element to the set.

        INPUT:

        - ``x`` - the element to add.

        - ``id`` - the ID to use for the entry (default: ``None``).

        - ``store`` - whether to write the data to the database
          (must be a named parameter; default: ``discretezoo.WRITE_TO_DB``).

        - ``cur`` - the cursor to use for database interaction
          (must be a named parameter; default: ``None``).
        """
        store = lookup(kargs, "store", default = discretezoo.WRITE_TO_DB)
        cur = lookup(kargs, "cur", default = None)
        x, tx, id = self._normalize(x, id)
        if x in self:
            return
        if store:
            row = {c: tx[i] for i, c in enumerate(self._ordering)}
            row[self._foreign_key] = self._objid
            id = self._insert_row(self.__class__, row, cur = cur)
        self[x] = id

    def clear(self, **kargs):
        r"""
        Remove all entries.

        INPUT:

        - ``store`` - whether to delete the data from the database
          (must be a named parameter; default: ``discretezoo.WRITE_TO_DB``).

        - ``cur`` - the cursor to use for database interaction
          (must be a named parameter; default: ``None``).
        """
        store = lookup(kargs, "store", default = discretezoo.WRITE_TO_DB)
        cur = lookup(kargs, "cur", default = None)
        if store:
            self._delete_rows(self.__class__, {self._foreign_key: self._objid},
                              cur = cur)
        dict.clear(self)

    def difference(self, *largs):
        r"""
        Return the difference of two or more sets as a new set.

        INPUT:

        - any number of collections as unnamed parameters.
        """
        return set(self).difference(*largs)

    def difference_update(self, *largs, **kargs):
        r"""
        Remove all elements of given sets from this set.

        INPUT:

        - any number of collections as unnamed parameters.

        - ``store`` - whether to delete the data from the database
          (must be a named parameter; default: ``discretezoo.WRITE_TO_DB``).

        - ``cur`` - the cursor to use for database interaction
          (must be a named parameter; default: ``None``).
        """
        store = lookup(kargs, "store", default = discretezoo.WRITE_TO_DB)
        cur = lookup(kargs, "cur", default = None)
        for other in largs:
            for x in other:
                self.discard(x, store = store, cur = cur)

    def discard(self, x, **kargs):
        r"""
        Remove an element from a set if it is a member.

        INPUT:

        - ``x`` - the element to remove.

        - ``store`` - whether to delete the data from the database
          (must be a named parameter; default: ``discretezoo.WRITE_TO_DB``).

        - ``cur`` - the cursor to use for database interaction
          (must be a named parameter; default: ``None``).
        """
        store = lookup(kargs, "store", default = discretezoo.WRITE_TO_DB)
        cur = lookup(kargs, "cur", default = None)
        try:
            self.remove(x, store = store, cur = cur)
        except KeyError:
            pass

    def intersection(self, *largs):
        r"""
        Return the intersection of two or more sets as a new set.

        INPUT:

        - any number of collections as unnamed parameters.
        """
        return set(self).intersection(*largs)

    def intersection_update(self, *largs, **kargs):
        r"""
        Update a set with the intersection of itself and given sets.

        INPUT:

        - any number of collections as unnamed parameters.

        - ``store`` - whether to delete the data from the database
          (must be a named parameter; default: ``discretezoo.WRITE_TO_DB``).

        - ``cur`` - the cursor to use for database interaction
          (must be a named parameter; default: ``None``).
        """
        store = lookup(kargs, "store", default = discretezoo.WRITE_TO_DB)
        cur = lookup(kargs, "cur", default = None)
        for x in set(self):
            if not all(x in other for other in largs):
                self.remove(x, store = store, cur = cur)

    def isdisjoint(self, other):
        r"""
        Return ``True`` if two sets have an empty intersection.

        INPUT:

        - ``other`` - the set to check whether it is disjoint with this set.
        """
        return set(self).isdisjoint(other)

    def issubset(self, other):
        r"""
        Report whether another set contains this set.

        INPUT:

        - ``other`` - the set to check whether this set is its subset.
        """
        return set(self).issubset(other)

    def issuperset(self, other):
        r"""
        Report whether this set contains another set.

        INPUT:

        - ``other`` - the set to check whether this set is its superset.
        """
        return set(self).issuperset(other)

    def pop(self, *largs, **kargs):
        r"""
        Remove and return an arbitrary set element.

        Raise ``KeyError`` if the set is empty.

        INPUT:

        - an unnamed parameter specifies the set element to pop.

        - a second unnamed parameter specifies the value to return if the
          first parameter does not exist in the set.

        - ``store`` - whether to delete the data from the database
          (must be a named parameter; default: ``discretezoo.WRITE_TO_DB``).

        - ``cur`` - the cursor to use for database interaction
          (must be a named parameter; default: ``None``).
        """
        store = lookup(kargs, "store", default = discretezoo.WRITE_TO_DB,
                       destroy = True)
        cur = lookup(kargs, "cur", default = None, destroy = True)
        if len(largs) == 0:
            try:
                x = next(iter(self))
            except StopIteration:
                raise KeyError('pop from an empty set')
        else:
            x = largs[0]
        try:
            self.remove(x, store = store, cur = cur)
            return x
        except KeyError as ex:
            if len(largs) > 1:
                return largs[1]
            else:
                raise ex

    def popitem(self, **kargs):
        r"""
        Remove and return some (value, ID) pair, where the ID is the ID of the
        entry.

        Raise ``KeyError`` if the set is empty.

        INPUT:

        - ``store`` - whether to delete the data from the database
          (must be a named parameter; default: ``discretezoo.WRITE_TO_DB``).

        - ``cur`` - the cursor to use for database interaction
          (must be a named parameter; default: ``None``).
        """
        store = lookup(kargs, "store", default = discretezoo.WRITE_TO_DB)
        cur = lookup(kargs, "cur", default = None)
        k, v = dict.popitem(self)
        if store:
            try:
                self._delete_rows(self.__class__,
                                  {self._spec["primary_key"]: v}, cur = cur)
            except self._db.exceptions as ex:
                self[k] = v
                raise ex
        return (k, v)

    def remove(self, x = None, id = None, **kargs):
        r"""
        Remove an element from a set.

        If the element is not a member, raise a ``KeyError``.

        INPUT:

        - ``x`` - the element to remove (default: ``None``).

        - ``id`` - the ID of the entry to delete (default: ``None``). Only used
          if ``x`` is ``None``.

        - ``store`` - whether to delete the data from the database
          (must be a named parameter; default: ``discretezoo.WRITE_TO_DB``).

        - ``cur`` - the cursor to use for database interaction
          (must be a named parameter; default: ``None``).
        """
        store = lookup(kargs, "store", default = discretezoo.WRITE_TO_DB)
        cur = lookup(kargs, "cur", default = None)
        if x is None:
            if id is None:
                raise KeyError("element or ID not specified")
            try:
                x = next(k for k in self if self[k] == id)
            except StopIteration:
                raise ValueError(id)
        else:
            x, tx, id = self._normalize(x, id)
            if x not in self:
                raise KeyError(x)
            id = self[x]
        if store:
            self._delete_rows(self.__class__, {self._spec["primary_key"]: id},
                              cur = cur)
        del self[x]

    def rename(self, old, new = None, id = None, **kargs):
        r"""
        Replace a set member with another value.

        If the new value already is a member, raise a ``KeyError``.

        INPUT:

        - ``old`` - the element to replace.

        - ``new`` - the new value. If ``None`` (default), treat ``old`` as the
          new value and replace the element identified by ``id``.

        - ``id`` - the ID of the entry to replace (default: ``None``). Only
          used if ``new`` is ``None``.

        - ``store`` - whether to change the data in the database
          (must be a named parameter; default: ``discretezoo.WRITE_TO_DB``).

        - ``cur`` - the cursor to use for database interaction
          (must be a named parameter; default: ``None``).
        """
        store = lookup(kargs, "store", default = discretezoo.WRITE_TO_DB)
        cur = lookup(kargs, "cur", default = None)
        if new is None:
            if id is None:
                raise KeyError("new value or ID not specified")
            new = old
            try:
                old = next(k for k in self if self[k] == id)
            except StopIteration:
                raise ValueError(id)
        old, told, id = self._normalize(old, id)
        if old not in self:
            (old, told, id), new, tnew = self._normalize(new, id), old, told
            if old not in self:
                raise KeyError(new, old)
        else:
            new, tnew, id = self._normalize(new, id)
        if old == new:
            return
        if new in self:
            raise KeyError(new)
        id = self[old]
        if store:
            self._update_rows(self.__class__,
                            {c: tnew[i] for i, c in enumerate(self._ordering)},
                            {self._spec["primary_key"]: id}, cur = cur)
        del self[old]
        self[new] = id

    def symmetric_difference(self, other):
        r"""
        Return the symmetric difference of two sets as a new set.

        INPUT:

        - ``other`` - the set to make the symmetric difference with.
        """
        return set(self).symmetric_difference(other)

    def symmetric_difference_update(self, other, **kargs):
        r"""
        Update a set with the symmetric difference of itself and another.

        INPUT:

        - ``other`` - the set to make the symmetric difference with.

        - ``store`` - whether to update the data in the database
          (must be a named parameter; default: ``discretezoo.WRITE_TO_DB``).

        - ``cur`` - the cursor to use for database interaction
          (must be a named parameter; default: ``None``).
        """
        store = lookup(kargs, "store", default = discretezoo.WRITE_TO_DB)
        cur = lookup(kargs, "cur", default = None)
        if not isinstance(other, dict):
            other = {x: None for x in other}
        for x in other:
            if x in self:
                self.remove(x, store = store, cur = cur)
            else:
                self.add(x, other[x], store = store, cur = cur)

    def union(self, *largs):
        r"""
        Return the union of sets as a new set.

        INPUT:

        - any number of collections as unnamed parameters.
        """
        return set(self).union(*largs)

    def update(self, *largs, **kargs):
        r"""
        Update a set with the union of itself and others.

        INPUT:

        - any number of collections as unnamed parameters.

        - ``store`` - whether to store the data to the database
          (must be a named parameter; default: ``discretezoo.WRITE_TO_DB``).

        - ``cur`` - the cursor to use for database interaction
          (must be a named parameter; default: ``None``).
        """
        store = lookup(kargs, "store", default = discretezoo.WRITE_TO_DB)
        cur = lookup(kargs, "cur", default = None)
        if not isinstance(other, dict):
            other = {x: None for x in other}
        for other in largs:
            for x in other:
                if x not in self or self[x] is None:
                    self.add(x, other[x], store = store, cur = cur)

def ZooSet(parent, name, spec, use_tuples = None):
    r"""
    Construct a subclass of ``_ZooDict``.

    INPUT:

    - ``parent`` - the class whose property is being represented.

    - ``name`` - the name of the represented property.

    - ``spec`` - metaclass specification. The ``"params"`` field should contain
      a dictionary with the specification for value properties at key
      ``"fields"``. See the documentation in the ``spec/`` folder for more
      details.

    - ``use_tuples`` - whether to use tuples for value properties. If ``None``
      (default), use tuples when the number of value properties is not one.
    """
    fields = spec["params"]["fields"]
    if len(fields) != 1:
        use_tuples = True
    elif use_tuples is None:
        use_tuples = False
    id = "zooid"
    fkey = "%s_id" % parent._spec["name"]
    clsdict = {
        '__doc__': r"""
    Set object for the property ``%s`` of the class ``%s``.
    """ % (name, parent.__name__),
        '_use_tuples': use_tuples,
        '_ordering': sorted(fields.keys()),
        '_foreign_key': fkey,
        '_foreign_obj': parent,
        '_spec': {
            "name": "%s_%s" % (parent._spec["name"], name),
            "primary_key": id,
            "indices": [([fkey] + fields.keys(), {"unique"})],
            "skip": {fkey, "deleted"},
            "fields": {
                id: ZooEntity,
                fkey: parent,
                "deleted": bool,
            },
            "fieldparams": {
                fkey: {"not_null"},
                "deleted": {"not_null"}
            },
            "compute": {},
            "condition": {},
            "default": {}
        }
    }

    clsdict["_spec"]["fields"].update(fields)
    ZooSet = type("ZooSet", (_ZooSet,), clsdict)
    ZooSet._init_spec(ZooSet, spec)
    return ZooSet

register_type(ZooSet)
