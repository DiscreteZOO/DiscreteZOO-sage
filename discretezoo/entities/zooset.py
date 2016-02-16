import discretezoo
from .zooentity import ZooEntity
from .zooproperty import ZooProperty
from .zootypes import register_type
from ..db.query import Column
from ..db.query import ColumnSet
from ..db.query import Table
from ..db.query import enlist
from ..util.utility import lookup

class _ZooSet(dict, ZooProperty):
    _parent = None
    _objid = None
    _use_tuples = None

    def __init__(self, data, vals = None, **kargs):
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
            else:
                t = Table(self._spec["name"])
                cur = self._db.query([t], t, {self._foreign_key: data,
                                              "deleted": False},
                                     cur = kargs["cur"])
                r = cur.fetchone()
                while r is not None:
                    v = tuple([r[k] for k in self._ordering])
                    if not self._use_tuples:
                        v = v[0]
                    self[v] = r[self._spec["primary_key"]]
                    r = cur.fetchone()
            if kargs["commit"]:
                self._db.commit()

    def __getattr__(self, name):
        if name == self._spec["primary_key"]:
            return set(self.values())
        if name not in self._ordering:
            raise AttributeError(name)
        if not self._use_tuples:
            return {t for t in self}
        i = self._ordering.index(name)
        return {t[i] for t in self}

    def __repr__(self):
        return '{%s}' % ', '.join(sorted(self))

    def _unique_index(self):
        return self._spec["indices"][0][0]

    def _normalize(self, x, id):
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

    def add(self, x, id = None, **kargs):
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
        store = lookup(kargs, "store", default = discretezoo.WRITE_TO_DB)
        cur = lookup(kargs, "cur", default = None)
        if store:
            self._delete_rows(self.__class__, {self._foreign_key: self._objid},
                              cur = cur)
        dict.clear(self)

    def difference(self, other):
        return set(self).difference(other)

    def difference_update(self, other, **kargs):
        store = lookup(kargs, "store", default = discretezoo.WRITE_TO_DB)
        cur = lookup(kargs, "cur", default = None)
        for x in other:
            self.discard(x, store = store, cur = cur)

    def discard(self, x, **kargs):
        store = lookup(kargs, "store", default = discretezoo.WRITE_TO_DB)
        cur = lookup(kargs, "cur", default = None)
        try:
            self.remove(x, store = store, cur = cur)
        except KeyError:
            pass

    def intersection(self, other):
        return set(self).intersection(other)

    def intersection_update(self, other, **kargs):
        store = lookup(kargs, "store", default = discretezoo.WRITE_TO_DB)
        cur = lookup(kargs, "cur", default = None)
        for x in self:
            if x not in other:
                self.remove(x, store = store, cur = cur)

    def isdisjoint(self, other):
        return set(self).isdisjoint(other)

    def issubset(self, other):
        return set(self).issubset(other)

    def issuperset(self, other):
        return set(self).issuperset(other)

    def pop(self, *largs, **kargs):
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
        return set(self).symmetric_difference(other)

    def symmetric_difference_update(self, other, **kargs):
        store = lookup(kargs, "store", default = discretezoo.WRITE_TO_DB)
        cur = lookup(kargs, "cur", default = None)
        if not isinstance(other, dict):
            other = {x: None for x in other}
        for x in other:
            if x in self:
                self.remove(x, store = store, cur = cur)
            else:
                self.add(x, other[x], store = store, cur = cur)

    def union(self, other):
        return set(self).union(other)

    def update(self, other, **kargs):
        store = lookup(kargs, "store", default = discretezoo.WRITE_TO_DB)
        cur = lookup(kargs, "cur", default = None)
        if not isinstance(other, dict):
            other = {x: None for x in other}
        for x in other:
            if x not in self or self[x] is None:
                self.add(x, other[x], store = store, cur = cur)

def ZooSet(parent, name, spec, use_tuples = None):
    fields = spec["params"]["fields"]
    if len(fields) != 1:
        use_tuples = True
    elif use_tuples is None:
        use_tuples = False
    id = "zooid"
    fkey = "%s_id" % parent._spec["name"]

    class ZooSet(_ZooSet):
        _use_tuples = use_tuples
        _ordering = sorted(fields.keys())
        _foreign_key = fkey
        _foreign_obj = parent
        _spec = {
            "name": "%s_%s" % (parent._spec["name"], name),
            "primary_key": id,
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
            "default": {}
        }

    ZooSet._spec["fields"].update(fields)
    ZooSet._spec["indices"] = [([fkey] + fields.keys(), {"unique"})]
    ZooSet._init_spec(ZooSet, spec)
    return ZooSet

register_type(ZooSet)
