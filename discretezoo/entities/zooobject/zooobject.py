import re
from types import BuiltinFunctionType
from types import MethodType
import discretezoo
from ..change import Change
from ..zooentity import ZooEntity
from ..zooentity import ZooInfo
from ...db.query import Column
from ...db.query import Table
from ...util.utility import default
from ...util.utility import isinteger
from ...util.utility import lookup
from ...util.utility import update

class ZooObject(ZooEntity):
    _zooprops = None
    _spec = None
    _zooid = None
    _unique_id = None
    _unique_id_algorithm = None
    _parent = ZooEntity
    _dict = "_zooprops"
    _fields = None

    def __init__(self, data = None, **kargs):
        self._init_(ZooObject, kargs, setVal = {"data": data})

    def _init_defaults(self, d):
        default(d, "zooid")
        default(d, "unique_id")
        default(d, "unique_id_algorithm")

    def _parse_params(self, d):
        if ZooEntity._parse_params(self, d):
            return True
        elif isinstance(d["data"], basestring) \
                and re.match(r'^[0-9A-Fa-f]{64}$', d["data"]):
            d["unique_id"] = d["data"]
            d["data"] = None
            return True
        else:
            return False

    def _init_object(self, cl, d, setProp = {}):
        if self._zooid is None:
            self._zooid = d["zooid"]
        if self._unique_id is None:
            self._unique_id = d["unique_id"]
            self._unique_id_algorithm = d["unique_id_algorithm"]
        if self._zooid is None or self._unique_id is None:
            if d["cur"] is None:
                r = self._db_read(cl)
                self._zooid = r["zooid"]
                if self._unique_id is None:
                    uid = self._fields.unique_id
                    cur = self._db.query([uid.algorithm.column, uid.column],
                                         uid.getJoin(),
                                         {uid.foreign: self._zooid},
                                          limit = 1, cur = d["cur"])
                    r = cur.fetchone()
                    if r is not None:
                        self._unique_id_algorithm, self._unique_id = r
        ZooEntity.__init__(self, **d)

    def _copy_props(self, cl, obj):
        c = cl
        while c is not None:
            if isinstance(obj, c):
                self._setprops(c, obj._getprops(c))
            c = c._parent
        c = obj.__class__
        cl = self.__class__
        while c is not None and not issubclass(cl, c):
            self.__setattr__(c._dict, obj._getprops(c))
            self._extra_classes.add(c)
            c = c._parent
        for c in obj._extra_classes:
            try:
                self.__getattribute__(c._dict)
            except AttributeError:
                self.__setattr__(c._dict, obj.__getattribute__(c._dict))
                self._extra_classes.add(c)
        for a in dir(obj):
            if a not in dir(self):
                attr = obj.__getattribute__(a)
                if isinstance(attr, MethodType):
                    self.__setattr__(a, MethodType(attr.im_func, self, cl))

    def _db_read_nonprimary(self, cur = None):
        if self._unique_id is not None:
            uid = self._fields.unique_id
            query = {uid.column: self._unique_id}
            cur = self._db.query([Column(ZooObject._spec["primary_key"],
                                         table = ZooObject._spec["name"]),
                                  uid.algorithm.column],
                                 uid.getJoin(), query, cur = cur)
            r = cur.fetchone()
            if r is None:
                raise KeyError(query)
            self._zooid, self._unique_id_algorithm = r
            return True
        return False

    def _db_write_nonprimary(self, cur = None):
        uid = self.unique_id()
        uid.__setitem__(self._unique_id_algorithm, self._unique_id,
                        store = True, cur = cur)

    def _update_rows(self, cl, row, cond, cur = None, commit = None):
        if commit is None:
            commit = cur is None
        if cur is None:
            cur = self._db.cursor()
        self._db.query([cl._spec["primary_key"]] + row.keys(),
                       cl._spec["name"], cond, distinct = True, cur = cur)
        for r in cur.fetchall():
            for k, v in row.items():
                if v != r[k]:
                    Change(r[cl._spec["primary_key"]], cl, column = k,
                           cur = cur, db = self._db)
        self._db.update_rows(cl._spec["name"], row, cond, cur = cur,
                             commit = commit)

    def _add_change(self, cl, cur):
        Change(self._zooid, cl, cur = cur)

    def _getattr(self, name, parent):
        try:
            attr = parent.__getattribute__(self, name)
            error = False
        except AttributeError as ex:
            attr = None
            error = True
        if error or (isinstance(attr, MethodType) and
                (isinstance(attr.im_func, BuiltinFunctionType) or
                    (attr.func_globals["__package__"] is not None and
                     attr.func_globals["__package__"].startswith("sage.")) or
                    (attr.func_globals["__name__"] is not None and
                     attr.func_globals["__name__"].startswith("sage.")))):
            try:
                cl, name = self._getclass(name, alias = True)
            except KeyError:
                if error:
                    raise ex
                return attr
            def _attr(*largs, **kargs):
                store = lookup(kargs, "store",
                               default = discretezoo.WRITE_TO_DB,
                               destroy = True)
                cur = lookup(kargs, "cur", default = None, destroy = True)
                default = len(largs) + len(kargs) == 0
                props = self._getprops(cl)
                try:
                    if not default:
                        raise NotImplementedError
                    a = lookup(props, name)
                    if issubclass(cl._spec["fields"][name], ZooObject) \
                            and isinteger(a):
                        a = cl._spec["fields"][name](zooid = a)
                        update(props, name, a)
                    return a
                except (KeyError, NotImplementedError):
                    if error:
                        raise NotImplementedError
                    a = attr(*largs, **kargs)
                    if default:
                        if store:
                            self._update_rows(cl, {name: a},
                                    {self._spec["primary_key"]: self._zooid},
                                    cur = cur)
                        update(props, name, a)
                    return a
            _attr.func_name = name
            try:
                _attr.__doc__ = attr.__doc__
            except AttributeError:
                pass
            return _attr
        return attr

    def alias(self):
        try:
            return lookup(self._zooprops, "alias")
        except KeyError:
            self._zooprops["alias"] = ZooObject._spec["fields"]["alias"](self._zooid)
            return self._zooprops["alias"]

    def unique_id(self):
        try:
            return lookup(self._zooprops, "unique_id")
        except KeyError:
            self._zooprops["unique_id"] = ZooObject._spec["fields"]["unique_id"](self._zooid)
            return self._zooprops["unique_id"]

info = ZooInfo(ZooObject)
