r"""
A dictionary metaclass

This module provides a function to create dictionary-like classes.
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

class _ZooDict(dict, ZooProperty):
    r"""
    A generic dictionary class with database interaction.

    Extends Python"s ``dict`` class.
    """
    _parent = None
    _objid = None
    _use_key_tuples = None
    _use_val_tuples = None

    def __init__(self, data, vals = None, **kargs):
        r"""
        Object constructor.

        INPUT:

        - ``data`` - a ``_ZooDict`` instance to replicate, or the ID of the
          object for which properties are being represented.

        - ``vals`` - a dictionary of keys and values to be stored
          (default: ``None``). Only used if ``store`` is ``True``.

        - ``db`` - the database being used (must be a named parameter;
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
        if isinstance(data, _ZooDict):
            self._objid = data._objid
            dict.update(self, data)
            default(kargs, "db", data._db)
            ZooProperty._init_(self, kargs)
        else:
            ZooProperty._init_(self, kargs)
            self._objid = data
            if vals is not None and kargs["store"]:
                for k, v in vals.items():
                    self.__setitem__(k, v, store = True, cur = kargs["cur"])
                if kargs["commit"]:
                    self._db.commit()
            else:
                t = Table(self._spec["name"])
                cur = self._db.query([t], t, {self._foreign_key: data,
                                              "deleted": False},
                                     cur = kargs["cur"])
                for r in cur:
                    key = tuple([r[k] for k in self._key_ordering])
                    val = tuple([r[v] for v in self._val_ordering])
                    if not self._use_key_tuples:
                        key = key[0]
                    if not self._use_val_tuples:
                        val = val[0]
                    self.__setitem__(key, val,
                                     id = r[self._spec["primary_key"]],
                                     store = False)

    def __getattr__(self, name):
        r"""
        Get the values associated to the property with the given name.

        If the property is part of the key, a set of corresponding values is
        returned. If the property is part of the data, a list of corresponding
        values is returned.

        INPUT:

        - ``name`` - the name of the requested property.
        """
        if name == self._spec["primary_key"]:
            return {x[0] for x in dict.values(self)}
        if name in self._key_ordering:
            o = self._key_ordering
            v = self.keys()
            f = set
        elif name in self._val_ordering:
            o = self._val_ordering
            v = self.values()
            f = list
        else:
            raise AttributeError(name)
        if not self._use_key_tuples:
            return f(v)
        i = o.index(name)
        return f([t[i] for t in v])

    def __getitem__(self, k):
        r"""
        Get the value for the given key.

        INPUT:

        - ``k`` - the key to fetch the value for.
        """
        k, tk = self._normalize_key(k)
        return dict.__getitem__(self, k)[1]

    def __setitem__(self, k, v, id = None, **kargs):
        r"""
        Set a value for the given key.

        INPUT:

        - ``k`` - the key to set the value for.

        - ``v`` - the value to set.

        - ``id`` - the ID to use for the entry (default: ``None``).

        - ``store`` - whether to store the data to the database
          (must be a named parameter; default: ``discretezoo.WRITE_TO_DB``).

        - ``cur`` - the cursor to use for database interaction
          (must be a named parameter; default: ``None``).
        """
        store = lookup(kargs, "store", default = discretezoo.WRITE_TO_DB)
        cur = lookup(kargs, "cur", default = None)
        k, tk = self._normalize_key(k)
        v, tv = self._normalize_val(v)
        if k in self and self[k] == v:
            return
        if store:
            row = dict([(self._foreign_key, self._objid)] +
                    [(c, tk[i]) for i, c in enumerate(self._key_ordering)] +
                    [(c, tv[i]) for i, c in enumerate(self._val_ordering)])
            id = self._insert_row(self.__class__, row, cur = cur)
        dict.__setitem__(self, k, (id, v))

    def __delitem__(self, k = None, id = None, **kargs):
        r"""
        Delete the value for the given key.

        INPUT:

        - ``k`` - the key to delete the value for (default: ``None``).

        - ``id`` - the ID of the entry to delete (default: ``None``). Only used
          if ``k`` is ``None``.

        - ``store`` - whether to delete the data from the database
          (must be a named parameter; default: ``discretezoo.WRITE_TO_DB``).

        - ``cur`` - the cursor to use for database interaction
          (must be a named parameter; default: ``None``).
        """
        store = lookup(kargs, "store", default = discretezoo.WRITE_TO_DB)
        cur = lookup(kargs, "cur", default = None)
        if k is None:
            if id is None:
                raise KeyError("key or ID not specified")
            try:
                k = next(x for x in self if self[x][0] == id)
            except StopIteration:
                raise ValueError(id)
        else:
            k, tk = self._normalize_key(k)
            if k not in self:
                raise KeyError(k)
            id = dict.__getitem__(self, k)[0]
        if store:
            self._delete_rows(self.__class__, {self._spec["primary_key"]: id},
                              cur = cur)
        dict.__delitem__(self, k)

    def __repr__(self):
        r"""
        The object representation of the dictionary.
        """
        return "{%s}" % ", ".join(["%s: %s" % t for t in sorted(self.items())])

    def _unique_index(self):
        r"""
        Return a list of columns uniquely determining a row in the database.

        Returns a list of key columns.
        """
        return self._spec["indices"][0][0]

    def _normalize_key(self, k):
        r"""
        Normalize a key to the standard form.

        Returns a pair containing the key in the standard and tuple forms.

        INPUT:

        - ``k`` - the key to normalize.
        """
        tk = tuple(enlist(k))
        if not self._use_key_tuples and len(tk) == 1:
            k = tk[0]
        else:
            k = tk
        return (k, tk)

    def _normalize_val(self, v):
        r"""
        Normalize a value to the standard form.

        Returns a pair containing the value in the standard and tuple forms.

        INPUT:

        - ``v`` - the value to normalize.
        """
        tv = tuple(enlist(v))
        if not self._use_val_tuples and len(tv) == 1:
            v = tv[0]
        else:
            v = tv
        return (v, tv)

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
        col = None if cl._use_val_tuples else cl._val_ordering[0]
        return ColumnSet(cl, col, join = table,
            by = (("deleted", False),
                  (cl._foreign_key,
                   Column(cl._foreign_obj._spec["primary_key"],
                          table = table))),
            foreign = cl._foreign_key, ordering = cl._key_ordering)

    def _to_json(self):
        r"""
        Return an object suitable for conversion to JSON.

        Returns a ``dict`` with keys and values recursively converted
        to a suitable format.
        """
        return {to_json(k): to_json(v) for k, v in self.items()}

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

    def items(self):
        r"""
        List the (key, value) pairs.
        """
        return [(k, v) for k, (_, v) in dict.items(self)]

    def iteritems(self):
        r"""
        Return an iterator over the (key, value) pairs.
        """
        it = dict.iteritems(self)
        def iter():
            while True:
                k, (_, v) = next(it)
                yield (k, v)
        return iter()

    def itervalues(self):
        r"""
        Return an iterator over the values.
        """
        it = dict.itervalues(self)
        def iter():
            while True:
                yield next(it)[1]
        return iter()

    def pop(self, k, *largs, **kargs):
        r"""
        Remove specified key and return the corresponding value.

        If key is not found, the next argument is returned if given, otherwise
        ``KeyError`` is raised.

        INPUT:

        - ``k`` - the key to pop.

        - an unnamed parameter is returned if ``k`` is not found.

        - ``store`` - whether to delete the data from the database
          (must be a named parameter; default: ``discretezoo.WRITE_TO_DB``).

        - ``cur`` - the cursor to use for database interaction
          (must be a named parameter; default: ``None``).
        """
        store = lookup(kargs, "store", default = discretezoo.WRITE_TO_DB)
        cur = lookup(kargs, "cur", default = None)
        try:
            d = self[k]
            self.__delitem__(k, store = store, cur = cur)
            return d
        except KeyError as ex:
            if len(largs) > 0:
                return largs[0]
            else:
                raise ex

    def popitem(self, **kargs):
        r"""
        Remove and return some (key, value) pair.

        Raise ``KeyError`` if the dictionary is empty.

        INPUT:

        - ``store`` - whether to delete the data from the database
          (must be a named parameter; default: ``discretezoo.WRITE_TO_DB``).

        - ``cur`` - the cursor to use for database interaction
          (must be a named parameter; default: ``None``).
        """
        store = lookup(kargs, "store", default = discretezoo.WRITE_TO_DB)
        cur = lookup(kargs, "cur", default = None)
        k, (id, v) = dict.popitem(self)
        if store:
            try:
                self._delete_rows(self.__class__,
                                  {self._spec["primary_key"]: id}, cur = cur)
            except self._db.exceptions as ex:
                dict.__setitem__(self, k, (id, v))
                raise ex
        return (k, v)

    def setdefault(self, k, v = None, **kargs):
        r"""
        Get the value for the given key after setting it to the specified value
        if it had not existed.

        INPUT:

        - ``k`` - the key to get the value for.

        - ``v`` - the value to set if ``k`` is not found (default: ``None``).

        - ``store`` - whether to store the data to the database
          (must be a named parameter; default: ``discretezoo.WRITE_TO_DB``).

        - ``cur`` - the cursor to use for database interaction
          (must be a named parameter; default: ``None``).
        """
        store = lookup(kargs, "store", default = discretezoo.WRITE_TO_DB)
        cur = lookup(kargs, "cur", default = None)
        k, tk = self._normalize_key(k)
        if k in self:
            return self[k]
        else:
            self.__setitem__(k, v, store = store, cur = cur)

    def update(self, *largs, **kargs):
        r"""
        Update the dictionary from a dictionary or collection.

        INPUT:

        - an unnamed parameter should be a dictionary or collection of
          (key, value) pairs to update from.

        - ``store`` - whether to store the data to the database
          (must be a named parameter; default: ``discretezoo.WRITE_TO_DB``).

        - ``cur`` - the cursor to use for database interaction
          (must be a named parameter; default: ``None``).

        - any other named parameter will be added to the dictionary with its
          name as the key.
        """
        store = lookup(kargs, "store", default = discretezoo.WRITE_TO_DB,
                       destroy = True)
        cur = lookup(kargs, "cur", default = None, destroy = True)
        if len(largs) > 0:
            try:
                other = largs[0]
                for k in other:
                    self.__setitem__(k, other[k], store = store, cur = cur)
            except AttributeError:
                for k, v in other:
                    self.__setitem__(k, v, store = store, cur = cur)
        for k, v in kargs.items():
            self.__setitem__(k, v, store = store, cur = cur)

    def values(self):
        r"""
        Return a list of values.
        """
        return [v[1] for v in dict.values(self)]

def ZooDict(parent, name, spec, use_key_tuples = None, use_val_tuples = None):
    r"""
    Construct a subclass of ``_ZooDict``.

    INPUT:

    - ``parent`` - the class whose property is being represented.

    - ``name`` - the name of the represented property.

    - ``spec`` - metaclass specification. The ``"params"`` field should contain
      a dictionary with the specification for key properties at key ``"keys"``,
      and the specification for value properties at key ``"values"``. See the
      documentation in the ``spec/`` folder for more details.

    - ``use_key_tuples`` - whether to use tuples for key properties. If
      ``None`` (default), use tuples when the number of key properties is not
      one.

    - ``use_val_tuples`` - whether to use tuples for value properties. If
      ``None`` (default), use tuples when the number of value properties is not
      one.
    """
    keys = spec["params"]["keys"]
    values = spec["params"]["values"]
    if len(keys) != 1:
        use_key_tuples = True
    elif use_key_tuples is None:
        use_key_tuples = False
    if len(values) != 1:
        use_val_tuples = True
    elif use_val_tuples is None:
        use_val_tuples = False
    id = "zooid"
    fkey = "%s_id" % parent._spec["name"]
    clsdict = {
        "__doc__": r"""
    Dictionary object for the property ``%s`` of the class ``%s``.
    """ % (name, parent.__name__),
        "_use_key_tuples": use_key_tuples,
        "_use_val_tuples": use_val_tuples,
        "_key_ordering": sorted(keys.keys()),
        "_val_ordering": sorted(values.keys()),
        "_foreign_key": fkey,
        "_foreign_obj": parent,
        "_spec": {
            "name": "%s_%s" % (parent._spec["name"], name),
            "primary_key": id,
            "indices": [([fkey] + keys.keys(), {"unique"})],
            "skip": {fkey, "deleted"},
            "fields" : {
                id: ZooEntity,
                fkey: parent,
                "deleted": bool
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
    clsdict["_spec"]["fields"].update(keys)
    clsdict["_spec"]["fields"].update(values)
    ZooDict = type("ZooDict", (_ZooDict,), clsdict)
    ZooDict._init_spec(ZooDict, spec)
    return ZooDict

register_type(ZooDict)
