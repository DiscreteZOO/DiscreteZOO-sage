import discretezoo
from .zooentity import ZooEntity
from .zooproperty import ZooProperty
from .zootypes import register_type
from ..db.query import Column
from ..db.query import ColumnSet
from ..db.query import Table
from ..db.query import enlist
from ..util.utility import lookup

class _ZooDict(dict, ZooProperty):
    _parent = None
    _objid = None
    _use_key_tuples = None
    _use_val_tuples = None

    def __init__(self, data, vals = None, cur = None, db = None):
        dict.__init__(self)
        self._zooid = False
        if isinstance(data, _ZooDict):
            self._objid = data._objid
            dict.update(self, data)
            if db is None:
                db = data._db
            ZooProperty.__init__(self, db = db)
        else:
            ZooProperty.__init__(self, db = db)
            self._objid = data
            if vals is not None and cur is not None:
                for k, v in vals.items():
                    self.__setitem__(k, v, store = True, cur = cur)
            else:
                t = Table(self._spec["name"])
                cur = self._db.query([t], t, {self._foreign_key: data,
                                              "deleted": False},
                                     cur = cur)
                r = cur.fetchone()
                while r is not None:
                    key = tuple([r[k] for k in self._key_ordering])
                    val = tuple([r[v] for v in self._val_ordering])
                    if not self._use_key_tuples:
                        key = key[0]
                    if not self._use_val_tuples:
                        val = val[0]
                    self.__setitem__(key, val,
                                     id = r[self._spec["primary_key"]],
                                     store = False)
                    r = cur.fetchone()

    def __getattr__(self, name):
        if name == self._spec["primary_key"]:
            return {x[0] for x in dict.values(self)}
        if name in self._key_ordering:
            o = self._key_ordering
            v = self.keys()
            if not self._use_key_tuples:
                return set(v)
        elif name in self._val_ordering:
            o = self._val_ordering
            v = self.values()
            if not self._use_val_tuples:
                return v
        else:
            raise AttributeError(name)
        i = o.index(name)
        return [t[i] for t in v]

    def __getitem__(self, k):
        k, tk = self._normalize_key(k)
        return dict.__getitem__(self, k)[1]

    def __setitem__(self, k, v, id = None, store = discretezoo.WRITE_TO_DB,
                    cur = None):
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

    def __delitem__(self, k = None, id = None, store = discretezoo.WRITE_TO_DB,
                    cur = None):
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
        return '{%s}' % ', '.join(['%s: %s' % t for t in sorted(self.items())])

    def _unique_index(self):
        return self._spec["indices"][0][0]

    def _normalize_key(self, k):
        tk = tuple(enlist(k))
        if not self._use_key_tuples and len(tk) == 1:
            k = tk[0]
        else:
            k = tk
        return (k, tk)

    def _normalize_val(self, v):
        tv = tuple(enlist(v))
        if not self._use_val_tuples and len(tv) == 1:
            v = tv[0]
        else:
            v = tv
        return (v, tv)

    @staticmethod
    def _get_column(cl, name, table = None, join = None, by = None):
        col = None if cl._use_val_tuples else cl._val_ordering[0]
        if join is not None:
            if not isinstance(table, Table):
                table = join.join(Table(table), by = by)
        return ColumnSet(cl, col, join = table,
            by = (("deleted", False),
                  (cl._foreign_key,
                   Column(cl._foreign_obj._spec["primary_key"],
                          table = table))),
            foreign = cl._foreign_key, ordering = cl._key_ordering)

    def clear(self, store = discretezoo.WRITE_TO_DB, cur = None):
        if store:
            self._delete_rows(self.__class__, {self._foreign_key: self._objid},
                              cur = cur)
        dict.clear(self)

    def items(self):
        return [(k, v) for k, (_, v) in dict.items(self)]

    def iteritems(self):
        it = dict.iteritems(self)
        def iter():
            while True:
                k, (_, v) = next(it)
                yield (k, v)
        return iter

    def itervalues(self):
        it = dict.itervalues(self)
        def iter():
            while True:
                yield next(it)[1]
        return iter

    def pop(self, k, *largs, **kargs):
        store = lookup(kargs, "store", default = discretezoo.WRITE_TO_DB,
                       destroy = True)
        cur = lookup(kargs, "cur", default = None, destroy = True)
        try:
            d = self[k]
            self.__delitem__(k, store = store, cur = cur)
            return d
        except KeyError as ex:
            if len(largs) > 1:
                return largs[1]
            else:
                raise ex

    def popitem(self, store = discretezoo.WRITE_TO_DB, cur = None):
        k, (id, v) = dict.popitem(self)
        if store:
            try:
                self._delete_rows(self.__class__,
                                  {self._spec["primary_key"]: id}, cur = cur)
            except self._db.exceptions as ex:
                dict.__setitem__(self, k, (id, v))
                raise ex
        return (k, v)

    def setdefault(self, k, v = None, store = discretezoo.WRITE_TO_DB,
                   cur = None):
        k, tk = self._normalize_key(k)
        if k in self:
            return self[k]
        else:
            self.__setitem__(k, v, store = store, cur = cur)

    def update(self, k, v = None, store = discretezoo.WRITE_TO_DB, cur = None):
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
        for k in kargs:
            self.__setitem__(k, kargs[k], store = store, cur = cur)

    def values(self):
        return [v[1] for v in dict.values(self)]

def ZooDict(parent, name, spec, use_key_tuples = None, use_val_tuples = None):
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

    class ZooDict(_ZooDict):
        _use_key_tuples = use_key_tuples
        _use_val_tuples = use_val_tuples
        _key_ordering = sorted(keys.keys())
        _val_ordering = sorted(values.keys())
        _foreign_key = fkey
        _foreign_obj = parent
        _spec = {
            "name": "%s_%s" % (parent._spec["name"], name),
            "primary_key": id,
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
            "default": {}
        }

    ZooDict._spec["fields"].update(keys)
    ZooDict._spec["fields"].update(values)
    ZooDict._spec["indices"] = [([fkey] + keys.keys(), {"unique"})]
    ZooDict._init_spec(ZooDict, spec)
    return ZooDict

register_type(ZooDict)
