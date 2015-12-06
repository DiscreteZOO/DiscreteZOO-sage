import re
from types import MethodType
from ..query import Table
from ..utility import default
from ..utility import isinteger
from ..utility import lookup
from ..zooentity import ZooEntity
from ..zooentity import ZooInfo

class ZooObject(ZooEntity):
    _zooprops = None
    _spec = None
    _zooid = None
    _unique_id = None
    _parent = None
    _dict = "_zooprops"
    _extra_props = None
    _fields = None

    def __init__(self, data = None, **kargs):
        self._init_(ZooObject, kargs, setVal = {"data": data})

    def _init_(self, cl, d, defNone = [], defVal = {}, setVal = {},
               setProp = {}):
        self._extra_props = set()
        cl._init_defaults(self, d)
        for k in defNone:
            default(d, k)
        for k, v in defVal.items():
            default(d, k, v)
        for k, v in setVal.items():
            d[k] = v
        default(d, "db")
        ZooEntity.__init__(self, d["db"])
        if not cl._parse_params(self, d):
            self._init_params(d)
        cl._init_object(self, cl, d, setProp)
        self._default_props(cl)
        if d["cur"] is not None:
            self._db_write(cl, d["cur"])

    def _init_defaults(self, d):
        default(d, "zooid")
        default(d, "unique_id")

    def _parse_params(self, d):
        if isinteger(d["data"]):
            d["zooid"] = Integer(d["data"])
            d["data"] = None
            return True
        elif isinstance(d["data"], basestring) \
                and re.match(r'^[0-9A-Fa-f]{64}$', d["data"]):
            d["unique_id"] = Integer(d["data"])
            d["data"] = None
        else:
            return False

    def _init_params(self, d):
        pass

    def _init_skip(self, d):
        pass

    def _init_object(self, cl, d, setProp = {}):
        self._zooid = d["zooid"]
        self._unique_id = d["unique_id"]
        self._unique_id = self._db_read(cl)["unique_id"]

    def _default_props(self, cl):
        c = cl
        while c is not None:
            self._setprops(c, {})
            c = c._parent
        for c, m in cl._spec["default"].items():
            self._getprops(c).update(m)

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
            self._extra_props.add(c._dict)
            c = c._parent
        for p in obj._extra_props:
            try:
                self.__getattribute__(p)
            except AttributeError:
                self.__setattr__(p, obj.__getattribute__(p))
                self._extra_props.add(p)
        for a in dir(obj):
            if a not in dir(self):
                attr = obj.__getattribute__(a)
                if isinstance(attr, MethodType):
                    self.__setattr__(a, MethodType(attr.im_func, self, cl))

    def _db_read(self, cl, join = None, query = None, cur = None):
        if query is None:
            if self._zooid is None:
                if self._unique_id is not None:
                    query = {"unique_id": self._unique_id}
                    cur = self._db.query([ZooObject._spec["primary_key"]],
                                         Table(ZooObject._spec["name"]),
                                         query, cur = cur)
                    r = cur.fetchone()
                    if r is None:
                        raise KeyError(query)
                    self._zooid = r[0]
                raise IndexError("object id not given")
            query = {"id": self._zooid}
        t = Table(cl._spec["name"])
        if join is None:
            join = t
        cur = self._db.query([t], join, query, cur = cur)
        r = cur.fetchone()
        cur.close()
        if r is None:
            raise KeyError(query)
        self._setprops(cl, self._todict(r, skip = cl._spec["skip"],
                                        fields = cl._spec["fields"]))
        return r

    def _db_write(self, cl, cur):
        id = None
        if cl._parent is None:
            id = cl._spec["primary_key"]
        self._db.insert_row(cl._spec["name"],
                            dict(self._getprops(cl).items() + \
                                 [(k, self.__getattribute__(k)())
                                  for k in cl._spec["skip"]]),
                            cur = cur, id = id)
        if id is not None:
            self._zooid = self._db.lastrowid(cur)

    def load_db_data(self):
        cl = self.__class__
        while cl is not None:
            cl._db_read(self)
            cl = cl._parent

    def id(self):
        return self._zooid

    def unique_id(self):
        if self._unique_id is None:
            raise NotImplementedError
        return self._unique_id

info = ZooInfo(ZooObject)
