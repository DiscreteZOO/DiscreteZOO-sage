import graphzoo
from ..query import A as All
from ..query import And
from ..query import Column
from ..query import Count
from ..query import In
from ..query import Subquery
from ..query import Table
from ..utility import default
from ..utility import isinteger
from ..utility import lookup
from ..utility import todict
from ..utility import tomultidict

class ZooEntity(object):
    _baseprops = None
    _spec = None
    _db = None
    _dict = "_baseprops"
    _fields = None
    _parent = None

    def __init__(self, data = None, **kargs):
        self._init_(ZooEntity, kargs, setVal = {"data": data})

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
        default(d, "cur")
        self._initdb(d["db"])
        if not cl._parse_params(self, d):
            self._init_params(d)
        self._default_props(cl)
        cl._init_object(self, cl, d, setProp)
        if self._zooid is not False and d["cur"] is not None:
            self._db_write(cl, d["cur"])

    def setdb(self, db):
        self._db = db

    def _initdb(self, db = None):
        if self._db is not None:
            return
        if db is None:
            self._db = graphzoo.DEFAULT_DB
        else:
            self._db = db

    def _getprops(self, cl):
        return self.__getattribute__(cl._dict)

    def _setprops(self, cl, d):
        try:
            props = self.__getattribute__(cl._dict)
        except AttributeError:
            props = None
        if props is None:
            self.__setattr__(cl._dict, d)
        else:
            props.update(d)

    def _init_defaults(self, d):
        pass

    def _init_skip(self, d):
        pass

    def _parse_params(self, d):
        if isinteger(d["data"]):
            d["zooid"] = Integer(d["data"])
            d["data"] = None
            return True
        else:
            return False

    def _init_params(self, d):
        pass

    def _init_object(self, cl, d, setProp = {}):
        if self._zooid is None:
            self._zooid = d["zooid"]
        if self._zooid is None:
            if d["cur"] is None:
                r = self._db_read(cl)
                self._zooid = r["zooid"]

    def _default_props(self, cl):
        c = cl
        while c is not None:
            self._setprops(c, {})
            c = c._parent
        for c, m in cl._spec["default"].items():
            self._getprops(c).update(m)

    def _init_props(self, cl, d):
        if d["props"] is not None:
            self._init_skip(d)
            self._setprops(cl, self._todict(d["props"],
                                            skip = cl._spec["skip"],
                                            fields = cl._spec["fields"]))
            d["props"] = {k: v for k, v in d["props"].items()
                            if k not in cl._spec["fields"]
                                or k in cl._spec["skip"]}

    def _db_read(self, cl, join = None, query = None, cur = None):
        if query is None:
            if self._zooid is None:
                if not cl._db_read_nonprimary(self, cur = cur):
                    raise IndexError("object id not given")
            query = {"zooid": self._zooid}
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

    def _db_read_nonprimary(self, cur = None):
        return False

    def _db_write(self, cl, cur):
        id = None
        if cl._parent is None:
            id = cl._spec["primary_key"]
        row = dict(self._getprops(cl).items() +
                [(k, self.__getattribute__(k)()) for k in cl._spec["skip"]])
        if self._zooid is False:
            del row["zooid"]
        self._db.insert_row(cl._spec["name"], row, cur = cur, id = id)
        if id is not None:
            objid = self._db.lastrowid(cur)
            if self._zooid is not False:
                self._zooid = objid
            return objid

    def _todict(self, r, skip = [], fields = None):
        if fields is None:
            fields = self._spec["fields"]
        return {k: self._db.from_db_type(r[k],
                                lookup(fields, k, default = type(r[k])))
                for k in r.keys() if k in fields and k not in skip
                                     and r[k] is not None}

    @staticmethod
    def _get_column(cl, name, table = None, join = None, by = None):
        return Column(name, table = table, join = join, by = by)

    def load_db_data(self):
        cl = self.__class__
        while cl is not None:
            cl._db_read(self)
            cl = cl._parent

    def zooid(self):
        return self._zooid

class ZooInfo:
    cl = None

    def __init__(self, cl, db = None):
        self.cl = cl

    def __repr__(self):
        return "<%s at 0x%08x>" % (str(self), id(self))

    def __str__(self):
        return "Info object for %s" % self.cl

    def getdb(self):
        if self.cl._db is not None:
            return self.cl._db
        return graphzoo.DEFAULT_DB

    def initdb(self, db = None, commit = True):
        if db is None:
            db = self.getdb()
        for base in self.cl.__bases__:
            if issubclass(base, ZooEntity):
                ZooInfo(base).initdb(db = db, commit = False)
        if self.cl._spec is not None:
            db.init_table(self.cl._spec, commit = commit)

    def count(self, *largs, **kargs):
        db = lookup(kargs, "db", default = None, destroy = True)
        join = lookup(kargs, "join", default = None, destroy = True)
        by = lookup(kargs, "by", default = None, destroy = True)
        if db is None:
            db = self.getdb()
        t = Table(self.cl._spec["name"])
        if join is not None:
            t = join.join(t, by = by)
        if self.cl._parent is None:
            groupby = lookup(kargs, "groupby", default = [], destroy = True)
            if isinstance(groupby, set):
                groupby = list(groupby)
            elif not isinstance(groupby, list):
                groupby = [groupby]
            groupbycols = [Column(x, alias = True) for x in groupby]
            cond = And(*largs, **kargs)
            cols = t.getTables()
            for table, j, b in cond.getTables():
                if table not in cols:
                    t = t.join(table, by = b)
            cur = db.query(columns = [Count(Column(self.cl._spec["primary_key"],
                                                   self.cl._spec["name"]),
                                            distinct = True)] + groupbycols,
                           table = t, cond = cond, groupby = groupby)
            n = cur.fetchall()
            cur.close()
            return tomultidict(n, groupbycols)
        else:
            return ZooInfo(self.cl._parent).count(db = db, join = t,
                                        by = {self.cl._spec["primary_key"]},
                                        *largs, **kargs)

    def query(self, *largs, **kargs):
        db = lookup(kargs, "db", default = None, destroy = True)
        join = lookup(kargs, "join", default = None, destroy = True)
        by = lookup(kargs, "by", default = None, destroy = True)
        if db is None:
            db = self.getdb()
        t = Table(self.cl._spec["name"])
        if join is not None:
            t = join.join(t, by = by)
        if self.cl._parent is None:
            cur = lookup(kargs, "cur", default = None, destroy = True)
            orderby = lookup(kargs, "orderby", default = [], destroy = True)
            limit = lookup(kargs, "limit", default = None, destroy = True)
            offset = lookup(kargs, "offset", default = None, destroy = True)
            cond = And(*largs, **kargs)
            ct = cond.getTables()
            cols = t.getTables()
            columns = [Table(table) for table in cols]
            if cols.issuperset({tbl for tbl, j, b in ct}):
                return db.query(columns = columns, table = t, cond = cond,
                                orderby = orderby, limit = limit,
                                offset = offset, cur = cur)
            else:
                tt = Table(t)
                for tbl, j, b in ct:
                    if tbl not in cols:
                        t = t.join(tbl, by = b)
                c = Column(self.cl._spec["primary_key"], self.cl._spec["name"])
                return db.query(columns = columns, table = tt,
                                cond = In(c, Subquery(columns = [c], table = t,
                                                      cond = cond)),
                                orderby = orderby, limit = limit,
                                offset = offset, cur = cur)
        else:
            return ZooInfo(self.cl._parent).query(db = db, join = t,
                                        by = {self.cl._spec["primary_key"]},
                                        *largs, **kargs)

    def props(self, *largs, **kargs):
        db = lookup(kargs, "db", default = None, destroy = True)
        if db is None:
            db = self.getdb()
        cur = self.query(db = db, *largs, **kargs)
        return (todict(r, db) for r in cur)

    def all(self, *largs, **kargs):
        db = lookup(kargs, "db", default = None, destroy = True)
        if db is None:
            db = self.getdb()
        cur = self.query(db = db, *largs, **kargs)
        return (self.cl(todict(r, db), db = db) for r in cur)

    def one(self, *largs, **kargs):
        kargs["limit"] = 1
        db = lookup(kargs, "db", default = None, destroy = True)
        if db is None:
            db = self.getdb()
        cur = self.query(db = db, *largs, **kargs)
        r = cur.fetchone()
        if r is None:
            raise KeyError(largs, kargs)
        return self.cl(todict(r, db), db = db)
